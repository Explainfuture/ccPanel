"""Verify legacy entrypoints stop with a clear message before any external tool."""
import pathlib, shutil, subprocess, tempfile
root=pathlib.Path(__file__).resolve().parent
bash=shutil.which('bash')
assert bash, 'bash required'
with tempfile.TemporaryDirectory(prefix='ccpanel-retired-drill-') as empty:
 for name in ['commit-crash-check-remote.sh','checkpoint-live-check-remote.sh','pressure-live-check-remote.sh','retention-live-check-remote.sh']:
  result=subprocess.run([bash,str(root/name)],env={'PATH':empty},capture_output=True,text=True,timeout=5)
  assert result.returncode==64,(name,result.returncode)
  assert not result.stdout,name
  assert result.stderr.startswith('retired_staged_drill: '+name+';'),name
print('4 retired entrypoints rejected before external tools; runtime environment untouched')
