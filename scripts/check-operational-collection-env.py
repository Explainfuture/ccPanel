import importlib.util,pathlib,tempfile
spec=importlib.util.spec_from_file_location('collection_env',str(pathlib.Path(__file__).with_name('operational-collection-env.py')))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix='ccpanel-collection-env-') as folder:
    paths=[pathlib.Path(folder)/name for name in ('runtime.env','release.env')]
    def put(text):
        for path in paths:path.write_bytes(text.encode())
    put('SECRET=synthetic-only\nRETENTION_ENABLED=false\n')
    module.configure('set','false',list(map(str,paths)));module.configure('check','false',list(map(str,paths)))
    assert paths[0].read_bytes()==paths[1].read_bytes() and b'SECRET=synthetic-only' in paths[0].read_bytes()
    module.configure('set','true',list(map(str,paths)));module.configure('check','true',list(map(str,paths)))
    for text,mode,state in [('OPERATIONAL_COLLECTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED=true\n','set','false'),('OPERATIONAL_COLLECTION_ENABLED=FALSE\n','set','false'),('OPERATIONAL_COLLECTION_ENABLED=true\n','check','false'),('SECRET=x\n','set','invalid')]:
        put(text);before=[p.read_bytes() for p in paths]
        try:module.configure(mode,state,list(map(str,paths)));raise AssertionError('Expected rejection')
        except ValueError:pass
        assert before==[p.read_bytes() for p in paths]
    put('SECRET=x\n');paths[1].write_bytes(b'SECRET=y\n');before=[p.read_bytes() for p in paths]
    try:module.configure('set','false',list(map(str,paths)));raise AssertionError('Expected mismatch rejection')
    except ValueError:pass
    assert before==[p.read_bytes() for p in paths]
print('Collection env activation/deactivation, preservation and no-write rejection checks passed')
