// Explicit operator entry. Keep imports of the library free of CLI side effects
// when esbuild includes it in an actual-PG test bundle.
import {runCapacityWindowDb,windowDbConfigFromEnv,WindowDbError,type WindowDbCommand} from './capacity-window-db';
try{
  const args=process.argv.slice(2),[command,directory,sequence]=args;
  if(args.length!==(command==='status'?3:2))throw new WindowDbError('window_db_command_invalid');
  const value=await runCapacityWindowDb({command:command as WindowDbCommand,directory:directory??'',...(sequence===undefined?{}:{sequence})},windowDbConfigFromEnv(process.env));
  if(!value)throw new WindowDbError('window_db_operation_failed');
  console.log(JSON.stringify(value));if(value.status==='cleanup_deferred')process.exitCode=2;
}catch(error){console.error(JSON.stringify({code:error instanceof WindowDbError?error.code:'window_db_operation_failed',...(error instanceof WindowDbError&&error.diagnostic?{diagnostic:error.diagnostic}:{})}));process.exitCode=1;}
