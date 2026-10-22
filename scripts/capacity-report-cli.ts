import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {capacityReport} from './capacity-report';
const [samplesPath,loadPath,baselinePath,finalPath,output]=process.argv.slice(2);
if(!samplesPath||!loadPath||!baselinePath||!finalPath||!output)throw new Error('Usage: capacity-report samples.jsonl load.json baseline.json final.json output.json');
const load=JSON.parse(readFileSync(loadPath,'utf8')),baseline=JSON.parse(readFileSync(baselinePath,'utf8')),final=JSON.parse(readFileSync(finalPath,'utf8'));
const bytes=readFileSync(samplesPath);
if(!bytes.length||bytes.length>8*1024*1024||bytes.at(-1)!==10)throw new Error('incomplete_capacity_snapshot');
if(typeof final.samplesSha256!=='string'||createHash('sha256').update(bytes).digest('hex')!==final.samplesSha256)throw new Error('capacity_snapshot_mismatch');
const frames=bytes.toString('utf8').trim().split('\n').map(line=>JSON.parse(line));
if(final.loadFinishedAt!==Date.parse(load.completed_at))throw new Error('capacity_window_mismatch');
// Original sampler stores baseline containers and release.txt separately. The
// final metadata export may join release.txt as releaseStart without guessing.
const report=capacityReport({startedAt:Date.parse(load.started_at),finishedAt:Date.parse(load.completed_at),frames,baseline:baseline.containers??baseline,final:final.containers,releaseStart:baseline.release??final.releaseStart,releaseEnd:final.release});
writeFileSync(output,JSON.stringify(report,null,2),{mode:0o600,flag:'wx'});console.log(JSON.stringify(report));
if(!report.evidenceValid)process.exitCode=1;
