"""Update matching private env files without disclosing unrelated settings."""
import json,os,pathlib,sys,tempfile
KEY='OPERATIONAL_COLLECTION_ENABLED'
def configure(mode,state,names):
    if mode not in ('check','set') or state not in ('true','false') or len(names)!=2:raise ValueError('Invalid collection configuration arguments')
    paths=[pathlib.Path(name) for name in names]
    if any(not p.is_absolute() or p.is_symlink() or not p.is_file() for p in paths):raise ValueError('Expected two existing absolute env files')
    paths=[p.resolve() for p in paths]
    if paths[0]==paths[1]:raise ValueError('Expected distinct env files')
    sources=[p.read_bytes() for p in paths]
    if sources[0]!=sources[1]:raise ValueError('Env files differ; reconcile before changing collection')
    lines=sources[0].decode('utf-8').splitlines()
    values=[line.split('=',1)[1] for line in lines if line.startswith(KEY+'=')]
    if len(values)>1 or any(value not in ('true','false') for value in values):raise ValueError('Ambiguous collection configuration')
    if mode=='check':
        if values!=[state]:raise ValueError('Collection state differs from explicit requirement')
    else:
        text='\n'.join([line for line in lines if not line.startswith(KEY+'=')]+[KEY+'='+state])+'\n'
        for path in paths:
            fd,name=tempfile.mkstemp(prefix='.collection-env-',dir=str(path.parent))
            try:
                with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as file:
                    os.fchmod(file.fileno(),0o600) if hasattr(os,'fchmod') else None
                    file.write(text);file.flush();os.fsync(file.fileno())
                os.replace(name,str(path))
                if os.name!='nt':
                    directory=os.open(str(path.parent),os.O_RDONLY|os.O_DIRECTORY)
                    try:os.fsync(directory)
                    finally:os.close(directory)
            finally:
                if os.path.exists(name):os.unlink(name)
    return {'mode':mode,'collection_enabled':state=='true'}
if __name__=='__main__':
    try:print(json.dumps(configure(sys.argv[1],sys.argv[2],sys.argv[3:])))
    except (ValueError,IndexError):raise SystemExit('Collection configuration rejected; no credentials displayed')
