#!/usr/bin/env python3
"""Passo 1 (no host): lista as figurinhas dos últimos DIAS dias que estão no banco da Evolution e grava em
xr-crm/_figurinhas.json, para o passo 2 (figurinhas-traz.js, dentro do container) pôr no CRM.
   python3 figurinhas-lista.py [dias]"""
import json
import subprocess
import sys

DIAS = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 7
SAIDA = "/docker/openclaw-cx8a/data/.openclaw/workspace/xr-crm/_figurinhas.json"


def psql(sql):
    u = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_USER"], capture_output=True, text=True).stdout.strip()
    d = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_DB"], capture_output=True, text=True).stdout.strip()
    r = subprocess.run(["docker", "exec", "-i", "evolution-db", "psql", "-U", u, "-d", d, "-At", "-q"], input=sql, capture_output=True, text=True)
    if r.returncode != 0 or "ERROR" in r.stderr:
        sys.exit("erro no banco da Evolution: " + r.stderr[:300])
    return [l for l in r.stdout.split(chr(10)) if l.strip()]


linhas = psql(f"""
SELECT json_build_object('jid', m.key->>'remoteJid', 'alt', m.key->>'remoteJidAlt', 'id', m.key->>'id', 'eu', m.key->>'fromMe',
  'part', COALESCE(m.key->>'participant', m.participant), 'nome', m."pushName", 'ts', m."messageTimestamp")::text
FROM "Message" m JOIN "Instance" i ON i.id = m."instanceId"
WHERE i.name = 'xr-whatsapp' AND m."messageType" = 'stickerMessage' AND m."messageTimestamp" > extract(epoch from now() - interval '{DIAS} days')
ORDER BY m."messageTimestamp";""")
itens = [json.loads(l) for l in linhas]
json.dump(itens, open(SAIDA, "w", encoding="utf-8"))
print("figurinhas dos últimos %d dias na Evolution: %d (lista em _figurinhas.json)" % (DIAS, len(itens)))
