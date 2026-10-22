"""Local bounded fixture checks; never touches a deployed release."""
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile

spec = importlib.util.spec_from_file_location('retain_assets', Path(__file__).with_name('retain-web-assets.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
root = Path(tempfile.mkdtemp(prefix='ccpanel-assets-')).resolve()
assert root.parent == Path(tempfile.gettempdir()).resolve() and root.name.startswith('ccpanel-assets-')
try:
    def build(name, files):
        web = root / name
        (web / 'assets').mkdir(parents=True)
        for file, value in files.items():
            (web / 'assets' / file).write_text(value, encoding='utf-8')
        return web

    first = build('first', {'old.js': 'old', 'shared.css': 'same'})
    second = build('second', {'current.js': 'current', 'shared.css': 'same'})
    assert module.retain(first, second) == {'owned': 2, 'previous': 2}
    assert (second / 'assets/old.js').read_text() == 'old'
    assert json.loads((second / module.MANIFEST).read_text()) == ['current.js', 'shared.css']
    third = build('third', {'latest.js': 'latest'})
    module.retain(second, third)
    assert (third / 'assets/current.js').is_file() and not (third / 'assets/old.js').exists()
    assert (first / 'assets/old.js').read_text() == 'old'
    conflicting = build('conflicting', {'current.js': 'different'})
    try:
        module.retain(second, conflicting)
        raise RuntimeError('collision_accepted')
    except AssertionError:
        assert (conflicting / 'assets/current.js').read_text() == 'different'
    (first / module.MANIFEST).write_text(json.dumps(['../outside.js']))
    try:
        module.retain(first, build('invalid', {'new.js': 'new'}))
        raise RuntimeError('traversal_accepted')
    except AssertionError:
        pass
    many = build('many', {f'file{i}.js': '' for i in range(513)})
    try:
        module.retain(second, many)
        raise RuntimeError('unbounded_assets_accepted')
    except AssertionError:
        pass
    print(json.dumps({'passed': True, 'previous_build_retained': True, 'history_bounded': True, 'collision_preserves_new_file': True, 'invalid_paths_rejected': True, 'file_count_bounded': True}))
finally:
    shutil.rmtree(root)
