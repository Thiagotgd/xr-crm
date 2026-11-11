#!/bin/sh
# Reinicia SÓ a cópia de ensaio do CRM (processos node cuja pasta de trabalho é _ensaio-crm). Roda dentro do container xr-apps.
E=/data/.openclaw/workspace/_ensaio-crm
for p in $(ls /proc | grep -E '^[0-9]+$'); do
  if [ "$(readlink /proc/$p/cwd 2>/dev/null)" = "$E" ]; then kill "$p" 2>/dev/null && echo "parou processo do ensaio $p"; fi
done
sleep 1
cd "$E" || exit 1
node --check server.js || exit 1
: > /tmp/evo-falsa.log; : > /tmp/cfaz-falso.log
nohup node evo-falsa.js 8197 /tmp/evo-falsa.log > /tmp/evo-falsa.out 2>&1 &
nohup node cfaz-falso.js 8198 /tmp/cfaz-falso.log > /tmp/cfaz-falso.out 2>&1 &
sleep 1
TZ=America/Sao_Paulo PORT=8196 EVO_BASE=http://127.0.0.1:8197 EVO_KEY=ensaio EVO_INSTANCE=ensaio ALERTA_INSTANCE=ensaio CFAZ_BASE=http://127.0.0.1:8198 CFAZ_API_TOKEN=ensaio SOMBRA=0 LEITOR=0 nohup node server.js > /tmp/crm-ensaio.log 2>&1 &
sleep 4
tail -n 3 /tmp/crm-ensaio.log; cat /tmp/cfaz-falso.out
