import pg from 'pg';
import {startOperationalCollector} from './operational-collector';
import {storeOperationalSnapshot} from './operational-history';
import {evaluateAndStoreOperationalAlerts} from './operational-alert-store';
import type {OperationalSnapshot} from './operational-snapshot';
import {operationalCollectionEnabled} from './operational-collection-config';

export function startOperationalCollection(snapshot:()=>OperationalSnapshot){
  if(!operationalCollectionEnabled(process.env))return async()=>{};
  // Dedicated one-connection pool: monitoring cannot occupy business pool slots.
  const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1,connectionTimeoutMillis:500,idleTimeoutMillis:10000,statement_timeout:1000,query_timeout:2000,application_name:'ccpanel_operational_history'});
  const failed=()=>console.error(JSON.stringify({code:'operational_history_write_failed'}));
  pool.on('error',failed);
  const stop=startOperationalCollector({snapshot,failed,write:async value=>{
    const client=await pool.connect();let broken=false;
    try{
      const result=await storeOperationalSnapshot(client,value);if(result.status==='conflict'||result.status==='full')console.error(JSON.stringify({code:'operational_history_write_rejected',reason:result.status}));
      try{const alerts=await evaluateAndStoreOperationalAlerts(client);if(alerts.status==='full'||alerts.status==='stale')console.error(JSON.stringify({code:'operational_alert_evaluation_rejected',reason:alerts.status}));}
      catch{broken=true;console.error(JSON.stringify({code:'operational_alert_evaluation_failed'}));}
    }
    catch(error){broken=true;throw error;}
    finally{client.release(broken);}
  }});
  return async()=>{await stop();await pool.end();};
}
