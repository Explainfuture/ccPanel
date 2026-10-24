// Deployment check before stopping the previous runtime. Validate the required
// directory mount, sample freshness and configuration without touching PG.
import { readHostTelemetry } from '../packages/server/src/host-telemetry';
import { pressurePolicy } from '../packages/server/src/ingest-pressure';
const policy=pressurePolicy(process.env),sample=await readHostTelemetry(process.env.HOST_TELEMETRY_PATH||'/telemetry/host.json'),now=Date.now();
if(sample.sampledAt>now+1000||now-sample.sampledAt>policy.maxDiskAgeMs)throw new Error('host_probe_stale');
console.log(JSON.stringify({ingest_preflight:true,sample_age_ms:now-sample.sampledAt}));
