import pg from 'pg';
import {startOperationalCollector} from './operational-collector';
import {storeOperationalSnapshot} from './operational-history';
import {evaluateAndStoreOperationalAlerts} from './operational-alert-store';
import type {OperationalSnapshot} from './operational-snapshot';
import {operationalCollectionEnabled} from './operational-collection-config';
import {storePressureObservation} from './pressure-history-store';
import type {PressureAlerts} from './pressure-alerts';
import {evaluateAndStoreDlqAlerts} from './dlq-alert-store';

export function startOperationalCollection(snapshot:()=>OperationalSnapshot,pressureAlerts?:()=>PressureAlerts){
  if(!operationalCollectionEnabled(process.env))return async()=>{};
  // Dedicated one-connection pool: monitoring cannot occupy business pool slots.
  const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1,connectionTimeoutMillis:500,idleTimeoutMillis:10000,statement_timeout:1000,query_timeout:2000,application_name:'ccpanel_operational_history'});
  const failed=()=>console.error(JSON.stringify({code:'operational_history_write_failed'}));
  pool.on('error',failed);
  const stop=startOperationalCollector({snapshot,failed,write:async value=>{
    // Capture the existing API cache before waiting for PG; never probe on this path.
    let pressure:PressureAlerts|undefined;
    if(value.scope==='api_instance_aggregate'&&pressureAlerts){try{pressure=pressureAlerts();}catch{console.error(JSON.stringify({code:'pressure_history_snapshot_failed'}));}}
    const client=await pool.connect();let broken=false;
    try{
      const result=await storeOperationalSnapshot(client,value);if(result.status==='conflict'||result.status==='full')console.error(JSON.stringify({code:'operational_history_write_rejected',reason:result.status}));
      try{const alerts=await evaluateAndStoreOperationalAlerts(client);if(alerts.status==='full'||alerts.status==='stale')console.error(JSON.stringify({code:'operational_alert_evaluation_rejected',reason:alerts.status}));}
      catch{broken=true;console.error(JSON.stringify({code:'operational_alert_evaluation_failed'}));}
      if(pressure&&!broken){
        try{
          const stored=await storePressureObservation(client,{instance_id:value.instance_id,started_at:value.started_at,assessment:pressure});
          if(!['stored','duplicate','busy'].includes(stored.status))console.error(JSON.stringify({code:'pressure_history_write_rejected',reason:stored.status}));
        }catch{broken=true;console.error(JSON.stringify({code:'pressure_history_write_failed'}));}
      }
      if(value.scope==='api_instance_aggregate'&&!broken){
        try{const stored=await evaluateAndStoreDlqAlerts(client);if(!['stored','duplicate','busy'].includes(stored.status))console.error(JSON.stringify({code:'dlq_alert_write_rejected',reason:stored.status}));}
        catch{broken=true;console.error(JSON.stringify({code:'dlq_alert_write_failed'}));}
      }
    }
    catch(error){broken=true;throw error;}
    finally{client.release(broken);}
  }});
  return async()=>{await stop();await pool.end();};
}
