#!/usr/bin/env python3
"""Traz para o CRM as citações antigas (mensagem que responde a outra) guardadas no banco da Evolution. Roda no host.
   python3 importa-citacoes.py [caminho do crm.db]            → só mostra o que faria
   python3 importa-citacoes.py [caminho do crm.db] aplicar    → grava
Para cada mensagem do CRM que no WhatsApp era uma resposta, grava qual mensagem foi citada (citacao_wa_id) e, se
ainda não houver, o trecho citado (citacao_texto / citacao_de). Não mexe no texto das mensagens."""
import json
import sqlite3
import subprocess
import sys

args = [a for a in sys.argv[1:] if a != "aplicar"]
APLICAR = "aplicar" in sys.argv[1:]
CRM = args[0] if args else "/docker/openclaw-cx8a/data/.openclaw/workspace/xr-crm/crm.db"


def psql(sql):
    u = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_USER"], capture_output=True, text=True).stdout.strip()
    d = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_DB"], capture_output=True, text=True).stdout.strip()
    r = subprocess.run(["docker", "exec", "-i", "evolution-db", "psql", "-U", u, "-d", d, "-At", "-q"], input=sql, capture_output=True, text=True)
    if r.returncode != 0 or "ERROR" in r.stderr:
        sys.exit("erro no banco da Evolution: " + r.stderr[:300])
    # só quebra na quebra de linha comum: o texto citado pode ter outros separadores que o splitlines() também cortaria
    return [l for l in r.stdout.split(chr(10)) if l.strip()]


linhas = psql("""
SELECT json_build_object('id', m.key->>'id', 'alvo', m."contextInfo"->>'stanzaId',
  'conv', q->>'conversation', 'ext', q->'extendedTextMessage'->>'text',
  'img', (q ? 'imageMessage'), 'imgcap', q->'imageMessage'->>'caption',
  'doc', ((q ? 'documentMessage') OR (q ? 'documentWithCaptionMessage')), 'docnome', q->'documentMessage'->>'fileName',
  'aud', (q ? 'audioMessage'), 'vid', (q ? 'videoMessage'), 'fig', (q ? 'stickerMessage'))::text
FROM "Message" m JOIN "Instance" i ON i.id = m."instanceId"
CROSS JOIN LATERAL (SELECT (m."contextInfo"::jsonb)->'quotedMessage' AS q) x
WHERE i.name = 'xr-whatsapp' AND m."contextInfo"->>'stanzaId' IS NOT NULL AND q IS NOT NULL AND jsonb_typeof(q) = 'object';""")


def trecho(c):
    if c.get("conv"):
        return c["conv"]
    if c.get("ext"):
        return c["ext"]
    if c.get("img"):
        return "📷 Foto" + (" · " + c["imgcap"] if c.get("imgcap") else "")
    if c.get("doc"):
        return "📎 " + (c.get("docnome") or "Documento")
    if c.get("aud"):
        return "🎤 Áudio"
    if c.get("vid"):
        return "🎥 Vídeo"
    if c.get("fig"):
        return "Figurinha"
    return "Mensagem"


db = sqlite3.connect(CRM, timeout=15)
db.row_factory = sqlite3.Row
cont = {"na_evolution": len(linhas), "no_crm": 0, "id_novo": 0, "trecho_novo": 0, "original_no_crm": 0}
mudancas = []
for l in linhas:
    c = json.loads(l)
    if not c.get("id") or not c.get("alvo"):
        continue
    m = db.execute("SELECT id, citacao_texto, citacao_wa_id FROM mensagens WHERE wa_msg_id = ? LIMIT 1", (c["id"],)).fetchone()
    if not m:
        continue
    cont["no_crm"] += 1
    orig = db.execute("SELECT direcao FROM mensagens WHERE wa_msg_id = ? LIMIT 1", (c["alvo"],)).fetchone()
    if orig:
        cont["original_no_crm"] += 1
    novo_id = not (m["citacao_wa_id"] or "")
    novo_trecho = not (m["citacao_texto"] or "")
    if novo_id:
        cont["id_novo"] += 1
    if novo_trecho:
        cont["trecho_novo"] += 1
    if novo_id or novo_trecho:
        mudancas.append((m["id"], c["alvo"], trecho(c)[:300] if novo_trecho else None, (orig["direcao"] if orig else "") if novo_trecho else None))

print("respostas com citação na Evolution:", cont["na_evolution"], "| dessas, a mensagem está no CRM:", cont["no_crm"], "| a mensagem ORIGINAL também está no CRM:", cont["original_no_crm"])
print("passam a saber qual mensagem citaram:", cont["id_novo"], "| passam a mostrar o trecho citado (antes não mostravam):", cont["trecho_novo"])
if APLICAR:
    for mid, alvo, texto, de in mudancas:
        if texto is None:
            db.execute("UPDATE mensagens SET citacao_wa_id = ? WHERE id = ?", (alvo, mid))
        else:
            db.execute("UPDATE mensagens SET citacao_wa_id = ?, citacao_texto = ?, citacao_de = ? WHERE id = ?", (alvo, texto, de, mid))
    db.commit()
    print("APLICADO:", len(mudancas), "mensagens atualizadas. Com citação no CRM agora:", db.execute("SELECT COUNT(*) FROM mensagens WHERE COALESCE(citacao_texto,'') <> ''").fetchone()[0])
else:
    print("SÓ MOSTREI (nada gravado)")
