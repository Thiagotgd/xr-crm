#!/usr/bin/env python3
"""Grupos do WhatsApp no CRM: arrumação inicial (roda no host da VPS, uma vez).
   python3 arruma-grupos.py            → só mostra o que faria
   python3 arruma-grupos.py aplicar    → grava
1) Tira as mensagens de grupo que tinham entrado repetidas (mesmo grupo, remetente, texto e segundo; fica a primeira).
2) O que a própria recepção escreveu pelo celular estava gravado como se fosse de outra pessoa: passa a ser "Eu".
3) Põe no CRM todos os grupos de que o WhatsApp da recepção participa.
4) Para os grupos novos, traz do banco da Evolution o texto das mensagens dos últimos 30 dias (sem os arquivos).
5) Marca todos os grupos como lidos, para o contador começar do zero."""
import json
import sqlite3
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

APLICAR = "aplicar" in sys.argv[1:]
CRM_DB = "/docker/openclaw-cx8a/data/.openclaw/workspace/xr-crm/crm.db"
CRM = "http://172.18.0.3:8095"
DIAS = 30


def psql(sql):
    u = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_USER"], capture_output=True, text=True).stdout.strip()
    d = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_DB"], capture_output=True, text=True).stdout.strip()
    r = subprocess.run(["docker", "exec", "-i", "evolution-db", "psql", "-U", u, "-d", d, "-At", "-q"], input=sql, capture_output=True, text=True)
    if r.returncode != 0 or "ERROR" in r.stderr:
        sys.exit("erro no banco da Evolution: " + r.stderr[:300])
    return [l for l in r.stdout.split(chr(10)) if l.strip()]


def api(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(CRM + path, data=data, method="POST" if body is not None else "GET", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=90) as resp:
        return json.loads(resp.read() or b"null")


db = sqlite3.connect(CRM_DB, timeout=20)
db.row_factory = sqlite3.Row

# 1) repetidas (só as antigas, sem identificador do WhatsApp)
tem_wa = any(c[1] == "wa_msg_id" for c in db.execute("PRAGMA table_info(grupo_mensagens)"))
filtro = "COALESCE(wa_msg_id,'') = ''" if tem_wa else "1=1"
rep = db.execute(f"SELECT COUNT(*) FROM grupo_mensagens WHERE {filtro} AND id NOT IN (SELECT MIN(id) FROM grupo_mensagens WHERE {filtro} GROUP BY grupo_id, remetente, texto, tipo, created_at)").fetchone()[0]
print("1) mensagens de grupo repetidas a tirar:", rep, "de", db.execute("SELECT COUNT(*) FROM grupo_mensagens").fetchone()[0])

# 2) mensagens da própria recepção gravadas com o endereço dela no grupo
proprios = [r[0] for r in db.execute("SELECT remetente FROM grupo_mensagens WHERE remetente LIKE '%@lid' AND remetente_nome = 'XR Diagnóstico Veterinário' GROUP BY remetente")]
n_prop = db.execute("SELECT COUNT(*) FROM grupo_mensagens WHERE remetente IN (%s)" % ",".join("?" * len(proprios)), proprios).fetchone()[0] if proprios else 0
print("2) mensagens da própria recepção que passam a aparecer como enviadas:", n_prop, "(endereços próprios:", len(proprios), ")")

# 3) grupos que faltam
disp = api("/api/grupos/disponiveis")
faltam = [g for g in disp if not g.get("monitorando")]
print("3) grupos do WhatsApp:", len(disp), "| já no CRM:", len(disp) - len(faltam), "| a acrescentar:", len(faltam))

# 4) histórico dos grupos novos
jids = [g["jid"] for g in faltam]
linhas = []
if jids:
    lista = ",".join("'" + j.replace("'", "") + "'" for j in jids)
    linhas = psql(f"""
SELECT json_build_object('jid', m.key->>'remoteJid', 'id', m.key->>'id', 'eu', m.key->>'fromMe', 'part', COALESCE(m.key->>'participant', m.participant), 'nome', m."pushName",
  'tipo', m."messageType", 'ts', m."messageTimestamp",
  'txt', COALESCE(m.message->>'conversation', m.message->'extendedTextMessage'->>'text', m.message->'imageMessage'->>'caption', m.message->'videoMessage'->>'caption'),
  'doc', COALESCE(m.message->'documentMessage'->>'fileName', m.message->'documentWithCaptionMessage'->'message'->'documentMessage'->>'fileName'),
  'doccap', COALESCE(m.message->'documentMessage'->>'caption', m.message->'documentWithCaptionMessage'->'message'->'documentMessage'->>'caption'))::text
FROM "Message" m JOIN "Instance" i ON i.id = m."instanceId"
WHERE i.name = 'xr-whatsapp' AND m.key->>'remoteJid' IN ({lista}) AND m."messageTimestamp" > extract(epoch from now() - interval '{DIAS} days')
ORDER BY m."messageTimestamp";""")
TIPOS = {"imageMessage": "image", "audioMessage": "audio", "documentMessage": "document", "documentWithCaptionMessage": "document", "videoMessage": "video",
         "conversation": "text", "extendedTextMessage": "text"}
BRT = timezone(timedelta(hours=-3))
historico = []
for l in linhas:
    c = json.loads(l)
    tipo = TIPOS.get(c.get("tipo") or "")
    if not tipo or not c.get("id"):
        continue
    texto = c.get("txt") or ""
    if tipo == "document":
        texto = "\n".join(x for x in [c.get("doc"), c.get("doccap")] if x)
    if not texto and tipo == "text":
        continue
    eu = str(c.get("eu")).lower() == "true"
    quando = datetime.fromtimestamp(int(c["ts"]), BRT).strftime("%Y-%m-%d %H:%M:%S")
    historico.append((c["jid"], "me" if eu else (c.get("part") or c.get("nome") or ""), "Eu" if eu else (c.get("nome") or ""), texto or "[%s]" % tipo, tipo, c["id"], quando))
print("4) mensagens dos últimos %d dias a trazer para os grupos novos:" % DIAS, len(historico), "em", len({h[0] for h in historico}), "grupos")

if not APLICAR:
    print("SÓ MOSTREI (nada gravado)")
    sys.exit(0)

if not tem_wa:
    sys.exit("o CRM novo ainda não está no ar (falta a coluna wa_msg_id): publique antes de aplicar")
db.execute(f"DELETE FROM grupo_mensagens WHERE {filtro} AND id NOT IN (SELECT MIN(id) FROM grupo_mensagens WHERE {filtro} GROUP BY grupo_id, remetente, texto, tipo, created_at)")
if proprios:
    db.execute("UPDATE grupo_mensagens SET remetente = 'me', remetente_nome = 'Eu' WHERE remetente IN (%s)" % ",".join("?" * len(proprios)), proprios)
db.commit()
for g in faltam:
    try:
        api("/api/grupo", {"jid": g["jid"], "nome": g["nome"]})
    except Exception as e:
        print("   não consegui acrescentar um grupo:", str(e)[:80])
ids = {r["jid"]: r["id"] for r in db.execute("SELECT id, jid FROM grupos WHERE ativo = 1")}
postas = 0
for jid, rem, nome, texto, tipo, wa, quando in historico:
    gid = ids.get(jid)
    if not gid or db.execute("SELECT 1 FROM grupo_mensagens WHERE wa_msg_id = ? LIMIT 1", (wa,)).fetchone():
        continue
    db.execute("INSERT INTO grupo_mensagens (grupo_id, remetente, remetente_nome, texto, tipo, wa_msg_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (gid, rem, nome, texto, tipo, wa, quando))
    postas += 1
db.execute("UPDATE grupos SET last_read_at = datetime('now','localtime')")
db.commit()
print("APLICADO: grupos no CRM agora:", len(ids), "| mensagens antigas trazidas:", postas, "| mensagens de grupo no total:", db.execute("SELECT COUNT(*) FROM grupo_mensagens").fetchone()[0])
