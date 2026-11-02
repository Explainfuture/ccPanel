import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {capacityWorkloadReport} from './capacity-workload-report';
const [input,output,expected='1800']=process.argv.slice(2);
if(!input||!output)throw new Error('Usage: capacity-workload-report final-load.json report.json expected-events');
const bytes=readFileSync(input);if(bytes.length>2*1024*1024)throw new Error('oversized_workload_evidence');
const report={...capacityWorkloadReport(JSON.parse(bytes.toString('utf8')),Number(expected)),load_sha256:createHash('sha256').update(bytes).digest('hex')};
writeFileSync(output,JSON.stringify(report,null,2),{flag:'wx',mode:0o600});console.log(JSON.stringify(report));
if(!report.evidenceValid||!report.dataLoadPass)process.exitCode=1;
