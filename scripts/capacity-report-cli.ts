import {readFileSync,writeFileSync} from 'node:fs';
import {capacityReport} from './capacity-report';
const [samplesPath,loadPath,baselinePath,finalPath,output]=process.argv.slice(2);
if(!samplesPath||!loadPath||!baselinePath||!finalPath||!output)throw new Error('Usage: capacity-report samples.jsonl load.json baseline.json final.json output.json');
const load=JSON.parse(readFileSync(loadPath,'utf8')),baseline=JSON.parse(readFileSync(baselinePath,'utf8')),final=JSON.parse(readFileSync(finalPath,'utf8'));
const frames=readFileSync(samplesPath,'utf8').trim().split('\n').map(line=>JSON.parse(line));
// Original sampler stores baseline containers and release.txt separately. The
// final metadata export may join release.txt as releaseStart without guessing.
const report=capacityReport({startedAt:Date.parse(load.started_at),finishedAt:Date.parse(load.completed_at),frames,baseline:baseline.containers??baseline,final:final.containers,releaseStart:baseline.release??final.releaseStart,releaseEnd:final.release});
writeFileSync(output,JSON.stringify(report,null,2),{mode:0o600,flag:'wx'});console.log(JSON.stringify(report));
if(!report.evidenceValid)process.exitCode=1;
