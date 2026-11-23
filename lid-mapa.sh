#!/bin/bash
# Gera xr-crm/lid-mapa.json (identificador @lid do WhatsApp -> telefone) a partir da Evolution desta VPS:
# tabela "Message" (key.remoteJid @lid com key.remoteJidAlt = telefone) e arquivos lid-mapping-* das instâncias.
# Roda no HOST por cron (a cada 10 min, 7-22h). Só lê a Evolution; só escreve o json na pasta do CRM.
set -u
CRM=/docker/openclaw-cx8a/data/.openclaw/workspace/xr-crm
VOL=/var/lib/docker/volumes/evolution_evolution_data/_data
URI=$(docker exec evolution-api printenv DATABASE_CONNECTION_URI 2>/dev/null) || { echo "lid-mapa: evolution-api fora"; exit 1; }
U=$(sed -E 's#.*//([^:]+):.*#\1#' <<<"$URI"); D=$(sed -E 's#.*/([^/?]+)(\?.*)?$#\1#' <<<"$URI")
TMP=$(mktemp)
docker exec evolution-db psql -U "$U" -d "$D" -At -F'|' -c "SELECT DISTINCT split_part(key->>'remoteJid','@',1), split_part(key->>'remoteJidAlt','@',1) FROM \"Message\" WHERE key->>'remoteJid' LIKE '%@lid' AND key->>'remoteJidAlt' LIKE '%@s.whatsapp.net'" > "$TMP" || { echo "lid-mapa: erro no psql"; rm -f "$TMP"; exit 1; }
python3 - "$CRM/lid-mapa.json" "$TMP" "$VOL" <<'PY'
import sys, os, json, glob, re
saida, txt, vol = sys.argv[1:4]
mapa = {}
try:
    with open(saida, encoding='utf-8') as f: mapa.update({k: v for k, v in json.load(f).items() if isinstance(v, str)})
except Exception: pass
antes = len(mapa)
def poe(lid, tel):
    lid = str(lid).split(':')[0]; tel = re.sub(r'\D', '', str(tel))
    if re.fullmatch(r'\d{8,20}', lid) and re.fullmatch(r'\d{8,15}', tel): mapa[lid] = tel
for linha in open(txt, encoding='utf-8', errors='replace'):
    if '|' in linha:
        lid, tel = linha.rstrip('\n').split('|', 1); poe(lid, tel)
for arq in glob.glob(os.path.join(vol, '*', 'lid-mapping-*_reverse.json')):
    lid = os.path.basename(arq)[len('lid-mapping-'):-len('_reverse.json')]
    try: poe(lid, json.load(open(arq)))
    except Exception: pass
for arq in glob.glob(os.path.join(vol, '*', 'lid-mapping-*.json')):
    nome = os.path.basename(arq)[len('lid-mapping-'):-len('.json')]
    if nome.endswith('_reverse'): continue
    try: poe(json.load(open(arq)), nome)
    except Exception: pass
tmp = saida + '.tmp'
with open(tmp, 'w', encoding='utf-8') as f: json.dump(mapa, f, sort_keys=True, separators=(',', ':'))
os.replace(tmp, saida)
print(f"lid-mapa: {len(mapa)} identificadores ({len(mapa) - antes:+d})")
PY
rm -f "$TMP"
