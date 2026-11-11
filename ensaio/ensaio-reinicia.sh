#!/bin/sh
# Reinicia SÓ a cópia de ensaio do CRM (processo node cuja pasta de trabalho é _ensaio-crm). Roda dentro do container xr-apps.
E=/data/.openclaw/workspace/_ensaio-crm
for p in $(ls /proc | grep -E '^[0-9]+$'); do
  if [ "$(readlink /proc/$p/cwd 2>/dev/null)" = "$E" ] && grep -qa "server.js" /proc/$p/cmdline 2>/dev/null; then
    kill "$p" && echo "parou o ensaio antigo (processo $p)"
  fi
done
sleep 1
cd "$E" || exit 1
node --check server.js || exit 1
TZ=America/Sao_Paulo PORT=8196 EVO_BASE=http://127.0.0.1:8197 EVO_KEY=ensaio EVO_INSTANCE=ensaio ALERTA_INSTANCE=ensaio SOMBRA=0 LEITOR=0 nohup node server.js > /tmp/crm-ensaio.log 2>&1 &
sleep 3
tail -4 /tmp/crm-ensaio.log
# o WhatsApp de mentira continua de pé?
if ! grep -qa "evo-falsa.js" /proc/[0-9]*/cmdline 2>/dev/null; then
  nohup node evo-falsa.js 8197 /tmp/evo-falsa.log > /tmp/evo-falsa.out 2>&1 &
  sleep 1; echo "WhatsApp de mentira religado"
fi
