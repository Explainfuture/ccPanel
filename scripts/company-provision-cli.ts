// Protected deployment env files only; never put passwords in command arguments.
import {pool} from '../packages/server/src/db';
import {initializePlatformOperator,provisionCompany,ProvisionError} from '../packages/server/src/company-provision';
try{
  if(process.env.CCPANEL_OPERATOR_MODE!=='true')throw new ProvisionError('explicit_deployment_operator_mode_required');
  const email=process.env.CCPANEL_OPERATOR_EMAIL||'';
  const mode=process.argv[2];
  if(mode==='initialize-operator')console.log(JSON.stringify(await initializePlatformOperator(email)));
  else if(mode==='create')console.log(JSON.stringify(await provisionCompany(email,process.env.CCPANEL_PROVISION_REQUEST_ID||'',{name:process.env.CCPANEL_COMPANY_NAME,owner_email:process.env.CCPANEL_OWNER_EMAIL,owner_password:process.env.CCPANEL_OWNER_PASSWORD})));
  else throw new ProvisionError('Usage: company-provision.mjs initialize-operator|create');
}catch(error){console.error(JSON.stringify({code:error instanceof ProvisionError?error.message:'company_provision_unavailable'}));process.exitCode=1;}
finally{await pool.end();}
