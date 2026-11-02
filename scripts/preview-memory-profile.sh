#!/usr/bin/env bash
# Reversible optimization only for the dedicated single-virtio-disk KVM preview.
# Does not alter Docker limits, JVM/PG settings, swap size or persistence rules.
set -euo pipefail
umask 077
mode=${1:-check}
record=/opt/ccpanel/shared/memory-profile-before.json
config=/etc/sysctl.d/99-ccpanel-preview-memory.conf
units=(fwupd.service fwupd-refresh.timer multipathd.service multipathd.socket)
case "$mode" in
  check)
    cat /proc/sys/vm/swappiness
    for unit in "${units[@]}"; do systemctl show "$unit" -p Id -p ActiveState -p UnitFileState; done
    exit 0;;
  apply|rollback) ;;
  *) echo 'Usage: preview-memory-profile.sh check|apply|rollback' >&2;exit 1;;
esac
test "$(id -u)" = 0
exec 9>/opt/ccpanel/shared/.memory-profile.lock
flock -n 9
if [ "$mode" = apply ]; then
  test "$(systemd-detect-virt)" = kvm
  python3 - <<'PY'
from pathlib import Path
import re,subprocess
assert Path('/sys/block/vda').is_dir(),'single virtio disk required'
assert not list(Path('/sys/block').glob('dm-*')),'device mapper present; review storage first'
root=subprocess.check_output(['findmnt','-n','-o','SOURCE','/'],text=True).strip()
assert re.fullmatch('/dev/vda[0-9]*',root),'root is not the expected virtio disk'
assert {tuple(line.split()) for line in subprocess.check_output(['lsblk','-ndo','NAME,TYPE'],text=True).splitlines()}<={('vda','disk'),('sr0','rom')},'unexpected block devices'
PY
  test ! -e "$record"
  test ! -e "$config"
  test "$(systemctl show fwupd-refresh.service -p ActiveState --value)" = inactive
  for unit in "${units[@]}"; do test "$(systemctl show "$unit" -p LoadState --value)" = loaded; done
  python3 - "$record" <<'PY'
import json,pathlib,subprocess,sys
units=['fwupd.service','fwupd-refresh.timer','multipathd.service','multipathd.socket']
before={'swappiness':int(pathlib.Path('/proc/sys/vm/swappiness').read_text()),'units':{}}
for unit in units:
 fields=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','UnitFileState'],text=True).splitlines())
 assert fields['ActiveState'] in ['active','inactive'] and fields['UnitFileState'] in ['enabled','disabled','static'],'unexpected initial unit state'
 before['units'][unit]=fields
with open(sys.argv[1],'x') as file:json.dump(before,file)
PY
  systemctl disable --now fwupd-refresh.timer multipathd.service multipathd.socket
  systemctl stop fwupd.service
  systemctl mask fwupd.service multipathd.service multipathd.socket
  printf 'vm.swappiness=10\n' >"$config"
  chmod 644 "$config"
  sysctl -w vm.swappiness=10
  echo 'KVM preview profile applied; previous state saved; security update timers unchanged'
else
  test -s "$record"
  if [ -e "$config" ]; then test "$(cat "$config")" = 'vm.swappiness=10'; fi
  original=$(python3 - "$record" <<'PY'
import json,sys
data=json.load(open(sys.argv[1]));value=data['swappiness'];assert isinstance(value,int) and 0<=value<=200;print(value)
PY
)
  systemctl unmask fwupd.service multipathd.service multipathd.socket
  python3 - "$record" <<'PY'
import json,subprocess,sys
data=json.load(open(sys.argv[1]));allowed={'fwupd.service','fwupd-refresh.timer','multipathd.service','multipathd.socket'}
assert set(data['units'])==allowed
for unit,state in data['units'].items():
 assert state['UnitFileState'] in ['enabled','disabled','static'] and state['ActiveState'] in ['active','inactive']
 if state['UnitFileState']=='enabled':subprocess.run(['systemctl','enable',unit],check=True)
 if state['UnitFileState']=='disabled':subprocess.run(['systemctl','disable',unit],check=True)
# Restore socket before daemon so activation dependencies match the old state.
for unit in ['multipathd.socket','multipathd.service','fwupd.service','fwupd-refresh.timer']:
 if data['units'][unit]['ActiveState']=='active':subprocess.run(['systemctl','start',unit],check=True)
PY
  rm -f "$config"
  sysctl -w "vm.swappiness=$original"
  echo 'Original unit and swappiness state restored; audit snapshot retained'
fi
