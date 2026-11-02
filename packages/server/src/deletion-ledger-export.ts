import type pg from 'pg';
import {parseDeletionLedger,type DeletionLedger} from './deletion-ledger-format';

// The caller owns an existing UTC READ ONLY REPEATABLE READ transaction.
// This reader does not start/end transactions, acquire pooled connections or
// log/export bytes; it can share the snapshot with other recovery-state reads.
export async function readDeletionLedgerSnapshot(db:Pick<pg.PoolClient,'query'>):Promise<DeletionLedger>{
  const exportedAt=(await db.query('SELECT transaction_timestamp() AS at')).rows[0].at.toISOString();
  const count=await db.query('SELECT count(*)::int AS count FROM deletion_barriers');
  if(count.rows[0].count>10000)throw new Error('deletion_ledger_budget_exceeded');
  const projects=(await db.query('SELECT p.tenant_id,p.id,p.privacy_epoch FROM projects p WHERE EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=p.tenant_id AND b.project_id=p.id) ORDER BY p.id')).rows;
  const jobs=(await db.query('SELECT j.id,j.tenant_id,j.project_id,j.user_id,j.scope,j.subject_hash,j.requested_at,j.blocked_before,j.stream_expires_at FROM deletion_jobs j WHERE EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.deletion_job_id=j.id) ORDER BY j.id')).rows;
  const barriers=(await db.query('SELECT tenant_id,project_id,identity_hash,deletion_job_id,requested_at,blocked_before FROM deletion_barriers ORDER BY project_id,identity_hash')).rows;
  const hasRetention=(await db.query('SELECT max(version)>=20 AS ready FROM schema_migrations')).rows[0].ready===true;
  const retention=hasRetention?(await db.query('SELECT tenant_id,id AS project_id,retention_days AS raw_days,retention_version AS version,retention_floor AS floor,project_retention_cutoff(retention_days,retention_floor) AS effective_from FROM projects ORDER BY id LIMIT 10001')).rows:undefined;
  if(retention&&retention.length>10000)throw new Error('deletion_ledger_budget_exceeded');
  const ledger={schema_version:hasRetention?2:1,exported_at:exportedAt,projects,jobs,barriers,...(retention?{retention}:{})};
  const encoded=JSON.stringify(ledger);
  if(Buffer.byteLength(encoded)>8*1024*1024)throw new Error('deletion_ledger_budget_exceeded');
  // Normalize PG Date values exactly as the existing JSON export path did.
  return parseDeletionLedger(JSON.parse(encoded));
}
