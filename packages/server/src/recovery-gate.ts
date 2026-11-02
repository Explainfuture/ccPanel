import { pool } from "./db";
export const recoveryLock = 169202617;
// A live service holds a shared session lease. Offline recovery must obtain the
// exclusive form, so a restore cannot race a service startup in the same DB.
export async function holdRecoveryLease() {
  const db = await pool.connect();
  try {
    await db.query("SELECT pg_advisory_lock_shared($1)", [recoveryLock]);
    const gate = await db.query(
      "SELECT blocked FROM recovery_gate WHERE id=true",
    );
    if (gate.rows[0]?.blocked !== false)
      throw new Error("recovery_gate_closed");
  } catch (e) {
    db.release(true);
    throw e;
  }
  const lost = () => {
    console.error(JSON.stringify({ code: "recovery_lease_lost" }));
    process.exit(1);
  };
  db.on("error", lost);
  return async () => {
    db.off("error", lost);
    try {
      await db.query("SELECT pg_advisory_unlock_shared($1)", [recoveryLock]);
      db.release();
    } catch {
      db.release(true);
    }
  };
}
