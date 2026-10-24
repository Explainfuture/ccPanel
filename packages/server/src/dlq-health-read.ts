import {withOperationalRead} from './operational-history-read';
import {queryDlqHealth} from './dlq-health-query';
export function readDlqHealth(user:string,tenant:string,project:string){return withOperationalRead(user,tenant,project,queryDlqHealth);}
