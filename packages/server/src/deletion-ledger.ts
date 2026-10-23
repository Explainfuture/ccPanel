// Operator-only CLI: redirect exports to a private file and encrypt off-host.
import { readSync } from "node:fs";
import { pool } from "./db";
import { recoveryLock } from "./recovery-gate";
import { lockProjectPrivacy } from "./privacy";
import { deleteBatch } from "./deletion-service";
import { digest } from "./security";
import { parseDeletionLedger } from "./deletion-ledger-format";
import {recoveredRetention} from './retention-recovery';

function readLedgerInput() {
  const chunks: Buffer[] = [];
  let total = 0;
  while (true) {
    const chunk = Buffer.alloc(8192),
      length = readSync(0, chunk);
    if (!length) break;
    total += length;
    if (total > 8 * 1024 * 1024)
      throw new Error("deletion_ledger_budget_exceeded");
    chunks.push(chunk.subarray(0, length));
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function run() {
  const mode = process.argv[2],
    db = await pool.connect();
  let locked = false;
  try {
    if (mode === "export") {
      await db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      const exportedAt = (
        await db.query("SELECT transaction_timestamp() AS at")
      ).rows[0].at.toISOString();
      const count = await db.query(
        "SELECT count(*)::int AS count FROM deletion_barriers",
      );
      if (count.rows[0].count > 10000)
        throw new Error("deletion_ledger_budget_exceeded");
      const projects = (
        await db.query(
          "SELECT p.tenant_id,p.id,p.privacy_epoch FROM projects p WHERE EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=p.tenant_id AND b.project_id=p.id) ORDER BY p.id",
        )
      ).rows;
      const jobs = (
        await db.query(
          "SELECT j.id,j.tenant_id,j.project_id,j.user_id,j.scope,j.subject_hash,j.requested_at,j.blocked_before,j.stream_expires_at FROM deletion_jobs j WHERE EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.deletion_job_id=j.id) ORDER BY j.id",
        )
      ).rows;
      const barriers = (
        await db.query(
          "SELECT tenant_id,project_id,identity_hash,deletion_job_id,requested_at,blocked_before FROM deletion_barriers ORDER BY project_id,identity_hash",
        )
      ).rows;
      const hasRetention=(await db.query('SELECT max(version)>=20 AS ready FROM schema_migrations')).rows[0].ready===true;
      const retention=hasRetention?(await db.query('SELECT tenant_id,id AS project_id,retention_days AS raw_days,retention_version AS version,retention_floor AS floor,project_retention_cutoff(retention_days,retention_floor) AS effective_from FROM projects ORDER BY id LIMIT 10001')).rows:undefined;
      if(retention&&retention.length>10000)throw new Error('deletion_ledger_budget_exceeded');
      const ledger = {
        schema_version: hasRetention?2:1,
        exported_at: exportedAt,
        projects,
        jobs,
        barriers,
        ...(retention?{retention}:{}),
      };
      const encoded = JSON.stringify(ledger);
      if (Buffer.byteLength(encoded) > 8 * 1024 * 1024)
        throw new Error("deletion_ledger_budget_exceeded");
      parseDeletionLedger(JSON.parse(encoded));
      await db.query("COMMIT");
      process.stdout.write(encoded + "\n");
      return;
    }
    if (mode === "check") {
      const state = await db.query(
        "SELECT blocked FROM recovery_gate WHERE id=true",
      );
      if (state.rows[0]?.blocked !== false)
        throw new Error("recovery_gate_closed");
      console.log(JSON.stringify({ gate: "open" }));
      return;
    }
    if (mode !== "apply" || process.env.CCPANEL_OFFLINE_RESTORE !== "true")
      throw new Error("offline_restore_required");
    locked = (
      await db.query("SELECT pg_try_advisory_lock($1) AS locked", [
        recoveryLock,
      ])
    ).rows[0].locked;
    if (!locked) throw new Error("services_still_online");
    // Remains closed after every failure, including invalid or stale input.
    await db.query(
      "INSERT INTO recovery_gate(id,blocked) VALUES(true,true) ON CONFLICT(id) DO UPDATE SET blocked=true,updated_at=clock_timestamp()",
    );
    const text = readLedgerInput();
    const ledger = parseDeletionLedger(JSON.parse(text));
    const ledgerHash = digest(text);
    const retentionHash=digest('retention:'+ledgerHash);
    const hasRetention=(await db.query('SELECT max(version)>=20 AS ready FROM schema_migrations')).rows[0].ready===true;
    if(hasRetention&&ledger.schema_version!==2)throw new Error('retention_ledger_required');
    if(!hasRetention&&ledger.schema_version===2)throw new Error('retention_schema_required');
    if(ledger.schema_version===2){
      const target=(await db.query('SELECT tenant_id,id,retention_days,retention_version FROM projects ORDER BY id LIMIT 10001')).rows;
      const source=new Map(ledger.retention.map(p=>[p.tenant_id+':'+p.project_id,p]));
      if(target.length!==source.size||target.some(p=>!source.has(p.tenant_id+':'+p.id)))throw new Error('restore_scope_metadata_missing');
      for(const p of target){const incoming=source.get(p.tenant_id+':'+p.id)!;if(incoming.version<p.retention_version)throw new Error('retention_ledger_version_stale');if(incoming.version===p.retention_version&&incoming.raw_days!==p.retention_days)throw new Error('retention_ledger_policy_conflict');}
      for(const p of ledger.retention){
        if((await db.query('SELECT 1 FROM recovery_import_scopes WHERE ledger_hash=$1 AND tenant_id=$2 AND project_id=$3',[retentionHash,p.tenant_id,p.project_id])).rowCount)continue;
        await db.query('BEGIN');await lockProjectPrivacy(db,p.project_id,true);
        const row=(await db.query('SELECT retention_days AS raw_days,retention_version AS version,retention_floor AS floor,clock_timestamp() AS now FROM projects WHERE tenant_id=$1 AND id=$2 FOR UPDATE',[p.tenant_id,p.project_id])).rows[0];
        if(!row)throw new Error('restore_scope_metadata_missing');
        const current={raw_days:row.raw_days,version:row.version,floor:row.floor?.toISOString()??null},next=recoveredRetention(current,p,row.now.getTime());
        await db.query('UPDATE projects SET retention_days=$3,retention_version=$4,retention_floor=$5,privacy_epoch=privacy_epoch+1 WHERE tenant_id=$1 AND id=$2',[p.tenant_id,p.project_id,next.raw_days,next.version,next.floor]);
        await db.query("UPDATE query_jobs SET status='cancelled',result=NULL,finished_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2",[p.tenant_id,p.project_id]);
        await db.query("INSERT INTO audit_logs(tenant_id,action,details) VALUES($1,'retention_ledger_applied',$2)",[p.tenant_id,JSON.stringify({project_id:p.project_id,source_exported_at:ledger.exported_at,before:current,after:next})]);
        await db.query('INSERT INTO recovery_import_scopes VALUES($1,$2,$3) ON CONFLICT DO NOTHING',[retentionHash,p.tenant_id,p.project_id]);await db.query('COMMIT');
      }
    }
    for (const p of ledger.projects) {
      const found = await db.query(
        "SELECT 1 FROM projects WHERE tenant_id=$1 AND id=$2",
        [p.tenant_id, p.id],
      );
      if (!found.rowCount) throw new Error("restore_scope_metadata_missing");
    }
    for (const p of ledger.projects) {
      const applied = await db.query(
        "SELECT 1 FROM recovery_import_scopes WHERE ledger_hash=$1 AND tenant_id=$2 AND project_id=$3",
        [ledgerHash, p.tenant_id, p.id],
      );
      if (applied.rowCount) continue;
      await db.query("BEGIN");
      await lockProjectPrivacy(db, p.id, true);
      for (const j of ledger.jobs.filter(
        (j) => j.tenant_id === p.tenant_id && j.project_id === p.id,
      )) {
        const old = await db.query(
          "SELECT tenant_id,project_id,scope,subject_hash FROM deletion_jobs WHERE id=$1",
          [j.id],
        );
        if (
          old.rows[0] &&
          (old.rows[0].tenant_id !== j.tenant_id ||
            old.rows[0].project_id !== j.project_id ||
            old.rows[0].scope !== j.scope ||
            old.rows[0].subject_hash !== j.subject_hash)
        )
          throw new Error("ledger_job_scope_mismatch");
        await db.query(
          "INSERT INTO deletion_jobs(id,tenant_id,project_id,user_id,scope,subject_hash,requested_at,blocked_before,stream_expires_at) VALUES($1,$2,$3,(SELECT id FROM users WHERE id=$4::uuid),$5,$6,$7,$8,GREATEST($9::timestamptz,clock_timestamp()+interval '45 days')) ON CONFLICT(id) DO UPDATE SET status='queued',error=NULL,finished_at=NULL,stream_expires_at=GREATEST(deletion_jobs.stream_expires_at,EXCLUDED.stream_expires_at)",
          [
            j.id,
            j.tenant_id,
            j.project_id,
            j.user_id,
            j.scope,
            j.subject_hash,
            j.requested_at,
            j.blocked_before,
            j.stream_expires_at,
          ],
        );
        await db.query('INSERT INTO audit_logs(tenant_id,action,details) VALUES($1,$2,$3)',[j.tenant_id,'deletion_restored_from_ledger',JSON.stringify({project_id:j.project_id,deletion_job_id:j.id,source_actor_id:j.user_id,scope:j.scope,subject_hash:j.subject_hash,source_exported_at:ledger.exported_at})]);
      }
      for (const b of ledger.barriers.filter(
        (b) => b.tenant_id === p.tenant_id && b.project_id === p.id,
      ))
        await db.query(
          "INSERT INTO deletion_barriers VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(project_id,identity_hash) DO UPDATE SET deletion_job_id=EXCLUDED.deletion_job_id,requested_at=GREATEST(deletion_barriers.requested_at,EXCLUDED.requested_at),blocked_before=GREATEST(deletion_barriers.blocked_before,EXCLUDED.blocked_before) WHERE deletion_barriers.blocked_before<=EXCLUDED.blocked_before",
          [
            b.tenant_id,
            b.project_id,
            b.identity_hash,
            b.deletion_job_id,
            b.requested_at,
            b.blocked_before,
          ],
        );
      await db.query(
        "UPDATE projects SET privacy_epoch=GREATEST(privacy_epoch,$3)+1 WHERE tenant_id=$1 AND id=$2",
        [p.tenant_id, p.id, p.privacy_epoch],
      );
      await db.query(
        "UPDATE query_jobs SET status='cancelled',result=NULL,finished_at=now() WHERE tenant_id=$1 AND project_id=$2",
        [p.tenant_id, p.id],
      );
      await db.query(
        "UPDATE write_keys SET revoked_at=COALESCE(revoked_at,now()) WHERE tenant_id=$1 AND project_id=$2 AND EXISTS(SELECT 1 FROM deletion_barriers WHERE tenant_id=$1 AND project_id=$2 AND identity_hash='*')",
        [p.tenant_id, p.id],
      );
      await db.query(
        "UPDATE deletion_jobs j SET status='queued',error=NULL,finished_at=NULL WHERE j.tenant_id=$1 AND j.project_id=$2 AND EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.deletion_job_id=j.id)",
        [p.tenant_id, p.id],
      );
      await db.query(
        "INSERT INTO audit_logs(tenant_id,action,details) VALUES($1,$2,$3)",
        [
          p.tenant_id,
          "deletion_ledger_applied",
          JSON.stringify({
            project_id: p.id,
            source_exported_at: ledger.exported_at,
          }),
        ],
      );
      await db.query(
        "INSERT INTO recovery_import_scopes VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
        [ledgerHash, p.tenant_id, p.id],
      );
      await db.query("COMMIT");
    }
    const deadline = Date.now() + 60000;
    let batches = 0;
    while (true) {
      const pending = await db.query(
        "SELECT id,tenant_id,project_id FROM deletion_jobs WHERE status IN ('queued','running') ORDER BY requested_at LIMIT 1",
      );
      const job = pending.rows[0];
      if (!job) break;
      if (Date.now() > deadline) throw new Error("recovery_cleanup_deadline");
      await db.query("BEGIN");
      await lockProjectPrivacy(db, job.project_id, true);
      const complete = await deleteBatch(db, job);
      batches++;
      await db.query(
        "UPDATE deletion_jobs SET status=$4,finished_at=CASE WHEN $4='completed' THEN clock_timestamp() ELSE NULL END WHERE tenant_id=$1 AND project_id=$2 AND id=$3",
        [
          job.tenant_id,
          job.project_id,
          job.id,
          complete ? "completed" : "running",
        ],
      );
      await db.query("COMMIT");
    }
    const failed = await db.query(
      "SELECT 1 FROM deletion_jobs WHERE status='failed' LIMIT 1",
    );
    if (failed.rowCount) throw new Error("recovery_cleanup_failed");
    await db.query("DELETE FROM recovery_import_scopes WHERE ledger_hash<>$1 AND ledger_hash<>$2", [
      ledgerHash,retentionHash,
    ]);
    await db.query(
      "UPDATE recovery_gate SET blocked=false,ledger_exported_at=$1,updated_at=clock_timestamp() WHERE id=true",
      [ledger.exported_at],
    );
    console.log(
      JSON.stringify({
        passed: true,
        projects: ledger.projects.length,
        barriers: ledger.barriers.length,
        cleanup_batches: batches,
        gate: "open",
        ledger_exported_at: ledger.exported_at,
      }),
    );
  } catch (e) {
    await db.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    let destroy = false;
    if (locked)
      try {
        await db.query("SELECT pg_advisory_unlock($1)", [recoveryLock]);
      } catch {
        destroy = true;
      }
    db.release(destroy);
    if (destroy) throw new Error("recovery_unlock_failed");
  }
}
run()
  .catch((e) => {
    console.error(
      JSON.stringify({
        error:
          typeof e.message === "string" && /^[a-z_]+$/.test(e.message)
            ? e.message
            : "deletion_ledger_failed",
        sqlstate:typeof e.code==='string'&&/^[0-9A-Z]{5}$/.test(e.code)?e.code:undefined,
      }),
    );
    process.exitCode = 1;
  })
  .finally(() => pool.end());
