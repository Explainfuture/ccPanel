import {it,expect} from 'vitest';
import {exportRecoverySourceState} from '../packages/server/src/recovery-state-export';

const base={sourceId:'source-fixture',expectedSchema:22,timeoutMs:1000};
// These fail before pg is constructed or any network operation starts.
// Constructor options alone do not prevent URI parameters overriding them.
it.each([
  'postgresql://operator@127.0.0.1/fixture?options=-c%20timezone%3DPacific%2FAuckland',
  'postgresql://operator@127.0.0.1/fixture?application_name=hidden',
  'postgresql://operator@127.0.0.1/fixture?statement_timeout=0',
  'postgresql://operator@127.0.0.1/fixture?sslmode=require&sslmode=disable',
  'postgresql://operator@127.0.0.1/fixture?sslmode=unknown',
  'postgresql://operator@127.0.0.1/fixture#ignored',
  'https://operator@127.0.0.1/fixture',
  'postgresql://127.0.0.1/fixture',
  'postgresql://operator@127.0.0.1/',
])('rejects unsupported source URI %s without exposing credentials',async connectionString=>{
  await expect(exportRecoverySourceState({...base,connectionString})).rejects.toMatchObject({code:'state_export_connection_invalid',message:'state_export_connection_invalid'});
});
