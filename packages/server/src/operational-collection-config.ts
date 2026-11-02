export function operationalCollectionEnabled(env:Record<string,string|undefined>){
  const value=env.OPERATIONAL_COLLECTION_ENABLED;
  if(value===undefined||value==='true')return true;
  if(value==='false')return false;
  throw new Error('invalid_operational_collection_configuration');
}
