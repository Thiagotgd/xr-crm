#!/usr/bin/env python3
"""Grupos do CRM: dá o identificador do WhatsApp às mensagens antigas (gravadas antes de 12/11/2026, sem ele), casando
cada uma com a mensagem igual no banco da Evolution (mesmo grupo, mesmo texto ou tipo de arquivo, mesmo lado e horário
com poucos segundos de diferença). Com o identificador, dá para reagir e responder citando essas mensagens.
Também traz a citação (qual mensagem foi respondida), quando houver.
   python3 casa-grupos.py [dias]            → só mostra
   python3 casa-grupos.py [dias] aplicar    → grava"""
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone

APLICAR = "aplicar" in sys.argv[1:]
nums = [a for a in sys.argv[1:] if a.isdigit()]
DIAS = int(nums[0]) if nums else 30
CRM_DB = "/docker/openclaw-cx8a/data/.openclaw/workspace/xr-crm/crm.db"
BRT = timezone(timedelta(hours=-3))


def psql(sql):
    u = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_USER"], capture_output=True, text=True).stdout.strip()
    d = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_DB"], capture_output=True, text=True).stdout.strip()
    r = subprocess.run(["docker", "exec", "-i", "evolution-db", "psql", "-U", u, "-d", d, "-At", "-q"], input=sql, capture_output=True, text=True)
    if r.returncode != 0 or "ERROR" in r.stderr:
        sys.exit("erro no banco da Evolution: " + r.stderr[:300])
    return [l for l in r.stdout.split(chr(10)) if l.strip()]


db = sqlite3.connect(CRM_DB, timeout=20)
db.row_factory = sqlite3.Row
grupos = {r["jid"]: r["id"] for r in db.execute("SELECT g.id, g.jid FROM grupos g WHERE g.ativo = 1 AND EXISTS (SELECT 1 FROM grupo_mensagens m WHERE m.grupo_id = g.id AND COALESCE(m.wa_msg_id,'') = '')")}
if not grupos:
    sys.exit("nenhum grupo com mensagem sem identificador")
lista = ",".join("'" + j.replace("'", "") + "'" for j in grupos)
linhas = psql(f"""
SELECT json_build_object('jid', m.key->>'remoteJid', 'id', m.key->>'id', 'eu', m.key->>'fromMe', 'tipo', m."messageType", 'ts', m."messageTimestamp",
  'txt', COALESCE(m.message->>'conversation', m.message->'extendedTextMessage'->>'text'),
  'alvo', m."contextInfo"->>'stanzaId',
  'qconv', (m."contextInfo"::jsonb)->'quotedMessage'->>'conversation', 'qext', (m."contextInfo"::jsonb)->'quotedMessage'->'extendedTextMessage'->>'text',
  'qimg', ((m."contextInfo"::jsonb)->'quotedMessage' ? 'imageMessage'), 'qdoc', (m."contextInfo"::jsonb)->'quotedMessage'->'documentMessage'->>'fileName',
  'qaud', ((m."contextInfo"::jsonb)->'quotedMessage' ? 'audioMessage'))::text
FROM "Message" m JOIN "Instance" i ON i.id = m."instanceId"
WHERE i.name = 'xr-whatsapp' AND m.key->>'remoteJid' IN ({lista}) AND m."messageTimestamp" > extract(epoch from now() - interval '{DIAS} days')
ORDER BY m."messageTimestamp";""")
TIPOS = {"imageMessage": "image", "audioMessage": "audio", "documentMessage": "document", "documentWithCaptionMessage": "document", "conversation": "text", "extendedTextMessage": "text"}

casadas = sem_par = ja = com_cita = 0
mud = []
usados = set()
for l in linhas:
    c = json.loads(l)
    tipo = TIPOS.get(c.get("tipo") or "")
    gid = grupos.get(c.get("jid"))
    if not tipo or not gid or not c.get("id"):
        continue
    if db.execute("SELECT 1 FROM grupo_mensagens WHERE wa_msg_id = ? LIMIT 1", (c["id"],)).fetchone():
        ja += 1
        continue
    texto = (c.get("txt") or "") if tipo == "text" else "[%s]" % tipo
    if not texto:
        continue
    t = datetime.fromtimestamp(int(c["ts"]), BRT)
    de, ate = (t - timedelta(seconds=20)).strftime("%Y-%m-%d %H:%M:%S"), (t + timedelta(seconds=45)).strftime("%Y-%m-%d %H:%M:%S")
    eu = str(c.get("eu")).lower() == "true"
    cand = db.execute("SELECT id, created_at FROM grupo_mensagens WHERE grupo_id = ? AND COALESCE(wa_msg_id,'') = '' AND tipo = ? AND texto = ? AND created_at BETWEEN ? AND ? AND (remetente = 'me') = ? ORDER BY created_at, id",
                      (gid, tipo, texto, de, ate, 1 if eu else 0)).fetchall()
    cand = [x for x in cand if x["id"] not in usados]
    if not cand:
        sem_par += 1
        continue
    alvo_t = t.strftime("%Y-%m-%d %H:%M:%S")
    melhor = min(cand, key=lambda x: abs((datetime.strptime(x["created_at"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(alvo_t, "%Y-%m-%d %H:%M:%S")).total_seconds()))
    usados.add(melhor["id"])
    casadas += 1
    cita = None
    if c.get("alvo"):
        trecho = c.get("qconv") or c.get("qext") or ("📷 Foto" if c.get("qimg") else None) or (("📎 " + c["qdoc"]) if c.get("qdoc") else None) or ("🎤 Áudio" if c.get("qaud") else None) or "Mensagem"
        cita = (c["alvo"], trecho[:300])
        com_cita += 1
    mud.append((melhor["id"], c["id"], cita))

print("grupos com mensagens antigas sem identificador:", len(grupos), "| mensagens desses grupos na Evolution (%d dias):" % DIAS, len(linhas))
print("casadas com uma mensagem do CRM:", casadas, "| já tinham identificador:", ja, "| sem par no CRM:", sem_par, "| com citação a trazer:", com_cita)
print("mensagens de grupo sem identificador hoje:", db.execute("SELECT COUNT(*) FROM grupo_mensagens WHERE COALESCE(wa_msg_id,'') = ''").fetchone()[0])
if APLICAR:
    for mid, wa, cita in mud:
        db.execute("UPDATE grupo_mensagens SET wa_msg_id = ? WHERE id = ? AND COALESCE(wa_msg_id,'') = ''", (wa, mid))
    db.commit()
    # a citação precisa saber de quem era a mensagem original (só depois de todos os identificadores gravados)
    for mid, wa, cita in mud:
        if not cita:
            continue
        orig = db.execute("SELECT remetente FROM grupo_mensagens WHERE wa_msg_id = ? LIMIT 1", (cita[0],)).fetchone()
        db.execute("UPDATE grupo_mensagens SET citacao_wa_id = ?, citacao_texto = ?, citacao_de = ? WHERE id = ? AND COALESCE(citacao_texto,'') = ''",
                   (cita[0], cita[1], ("out" if orig["remetente"] == "me" else "in") if orig else "", mid))
    db.commit()
    print("APLICADO. Sem identificador agora:", db.execute("SELECT COUNT(*) FROM grupo_mensagens WHERE COALESCE(wa_msg_id,'') = ''").fetchone()[0],
          "| com citação:", db.execute("SELECT COUNT(*) FROM grupo_mensagens WHERE COALESCE(citacao_texto,'') <> ''").fetchone()[0])
else:
    print("SÓ MOSTREI (nada gravado)")
