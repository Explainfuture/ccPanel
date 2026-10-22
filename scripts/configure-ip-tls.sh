#!/usr/bin/env bash
set -euo pipefail
# Run only after the release and nginx bootstrap exist. IP certificates use the
# shortlived profile; renewal is mandatory rather than a one-off setup step.
ip=${CCPANEL_PUBLIC_IP:?Set CCPANEL_PUBLIC_IP to the owned public IPv4 address}
python3 - "$ip" <<'PY'
import ipaddress,sys
address=ipaddress.ip_address(sys.argv[1])
if address.version != 4 or not address.is_global: raise SystemExit('A public IPv4 address is required')
PY
test "$(id -u)" = 0
test -f /opt/ccpanel/current/infra/nginx.conf
test -f /opt/ccpanel/shared/tls.crt
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-venv
python3 -m venv /opt/ccpanel/acme
/opt/ccpanel/acme/bin/pip install --disable-pip-version-check 'certbot==5.4.0'
install -d -m 755 /var/lib/ccpanel-acme
install -m 644 /opt/ccpanel/current/infra/nginx.conf /etc/nginx/sites-available/ccpanel
nginx -t
systemctl reload nginx

cat >/opt/ccpanel/shared/install-tls.sh <<'HOOK'
#!/usr/bin/env bash
set -euo pipefail
lineage=${RENEWED_LINEAGE:?Missing certificate lineage}
test -s "$lineage/fullchain.pem"
test -s "$lineage/privkey.pem"
openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt -untrusted "$lineage/fullchain.pem" "$lineage/cert.pem"
openssl x509 -in "$lineage/cert.pem" -checkend 86400 -noout
backup=$(mktemp -d /opt/ccpanel/shared/tls-backup.XXXXXX)
chmod 700 "$backup"
cp /opt/ccpanel/shared/tls.crt /opt/ccpanel/shared/tls.key "$backup/"
install -m 600 "$lineage/privkey.pem" /opt/ccpanel/shared/tls.key.next
install -m 644 "$lineage/fullchain.pem" /opt/ccpanel/shared/tls.crt.next
mv /opt/ccpanel/shared/tls.key.next /opt/ccpanel/shared/tls.key
mv /opt/ccpanel/shared/tls.crt.next /opt/ccpanel/shared/tls.crt
if nginx -t && systemctl reload nginx; then
  rm -f -- "$backup/tls.crt" "$backup/tls.key"
  rmdir -- "$backup"
else
  cp "$backup/tls.crt" "$backup/tls.key" /opt/ccpanel/shared/
  echo 'TLS installation failed; previous files restored. Backup retained for inspection.' >&2
  exit 1
fi
HOOK
chmod 700 /opt/ccpanel/shared/install-tls.sh

# Prove HTTP-01 routing using the staging CA before requesting a trusted cert.
/opt/ccpanel/acme/bin/certbot certonly --staging --non-interactive --agree-tos --register-unsafely-without-email \
  --preferred-profile shortlived --webroot -w /var/lib/ccpanel-acme --ip-address "$ip" \
  --cert-name ccpanel-ip-staging --config-dir /opt/ccpanel/acme-staging/config \
  --work-dir /opt/ccpanel/acme-staging/work --logs-dir /opt/ccpanel/acme-staging/logs
/opt/ccpanel/acme/bin/certbot certonly --non-interactive --agree-tos --register-unsafely-without-email \
  --preferred-profile shortlived --webroot -w /var/lib/ccpanel-acme --ip-address "$ip" \
  --cert-name ccpanel-ip --deploy-hook /opt/ccpanel/shared/install-tls.sh
openssl x509 -in /opt/ccpanel/shared/tls.crt -checkip "$ip" -noout

cat >/etc/systemd/system/ccpanel-cert-renew.service <<'UNIT'
[Unit]
Description=Renew ccPanel short-lived IP TLS certificate
After=network-online.target nginx.service
Wants=network-online.target
[Service]
Type=oneshot
ExecStart=/opt/ccpanel/acme/bin/certbot renew --quiet
Nice=10
UNIT
cat >/etc/systemd/system/ccpanel-cert-renew.timer <<'UNIT'
[Unit]
Description=Check ccPanel certificate renewal twice daily
[Timer]
OnCalendar=*-*-* 00,12:00:00
RandomizedDelaySec=1800
Persistent=true
[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable --now ccpanel-cert-renew.timer
systemctl list-timers ccpanel-cert-renew.timer --no-pager
curl -fsS "https://$ip/ready"
