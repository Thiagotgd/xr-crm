#!/usr/bin/env python3
"""Traz para o CRM as etiquetas do WhatsApp Business da recepção (roda no host da VPS).
   python3 importa-etiquetas.py            → só mostra o que faria
   python3 importa-etiquetas.py aplicar    → cria as etiquetas no CRM e marca os contatos
Lê do banco da Evolution (só leitura): quais etiquetas existem e em quais conversas estão. No CRM só ACRESCENTA
(etiqueta nova e marcação nova); não tira nem altera nada que já exista."""
import json
import sqlite3
import subprocess
import sys

APLICAR = len(sys.argv) > 1 and sys.argv[1] == "aplicar"
CRM = "/docker/openclaw-cx8a/data/.openclaw/workspace/xr-crm/crm.db"
# Filtros de lista do WhatsApp (não são etiquetas de verdade)
IGNORAR = {"no lidas", "não lidas", "favoritos", "grupos", ""}
# Cor de cada etiqueta no CRM (tons parecidos com os do WhatsApp, escuros o bastante para letra branca)
CORES = {"0": "#e0705f", "1": "#2e9be6", "2": "#d6a800", "3": "#b072c9", "4": "#6f8f9c", "5": "#2fa890", "6": "#d966d9", "7": "#b08a12", "8": "#5a69c2",
         "9": "#8f9e1c", "10": "#00a3b3", "11": "#e0787c", "12": "#4caf7a", "13": "#e03c3c", "14": "#0b8fd9", "15": "#5fae14", "16": "#e09400", "17": "#3aa7cf",
         "18": "#6b78e0", "19": "#8459c2"}


def psql(sql):
    u = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_USER"], capture_output=True, text=True).stdout.strip()
    d = subprocess.run(["docker", "exec", "evolution-db", "printenv", "POSTGRES_DB"], capture_output=True, text=True).stdout.strip()
    r = subprocess.run(["docker", "exec", "-i", "evolution-db", "psql", "-U", u, "-d", d, "-At", "-q"], input=sql, capture_output=True, text=True)
    if r.returncode != 0 or "ERROR" in r.stderr:
        sys.exit("erro no banco da Evolution: " + r.stderr[:300])
    return [l for l in r.stdout.splitlines() if l.strip()]


etiquetas = {}
for l in psql("""SELECT json_build_object('id', l."labelId", 'nome', l.name, 'cor', l.color)::text FROM "Label" l JOIN "Instance" i ON i.id = l."instanceId" WHERE i.name = 'xr-whatsapp';"""):
    e = json.loads(l)
    if (e["nome"] or "").strip().lower() not in IGNORAR:
        etiquetas[str(e["id"])] = e

conversas = [json.loads(l) for l in psql("""
SELECT json_build_object('jid', c."remoteJid", 'labels', c.labels,
  'tel', COALESCE(
     (SELECT m.key->>'remoteJidAlt' FROM "Message" m WHERE m."instanceId" = c."instanceId" AND m.key->>'remoteJid' = c."remoteJid" AND m.key->>'remoteJidAlt' LIKE '%@s.whatsapp.net' LIMIT 1),
     (SELECT w."remoteJid" FROM "IsOnWhatsapp" w WHERE (w.lid = c."remoteJid" OR w.lid = split_part(c."remoteJid", '@', 1)) AND w."remoteJid" LIKE '%@s.whatsapp.net' LIMIT 1)))::text
FROM "Chat" c JOIN "Instance" i ON i.id = c."instanceId"
WHERE i.name = 'xr-whatsapp' AND c.labels IS NOT NULL AND jsonb_typeof(c.labels) = 'array' AND jsonb_array_length(c.labels) > 0;""")]

db = sqlite3.connect(CRM, timeout=10)
db.row_factory = sqlite3.Row
contatos = {r["telefone"]: r["id"] for r in db.execute("SELECT id, telefone FROM contatos")}


def variantes(n):
    v = [n]
    if n.startswith("55") and len(n) == 13 and n[4] == "9":
        v.append(n[:4] + n[5:])
    if n.startswith("55") and len(n) == 12 and n[4] in "6789":
        v.append(n[:4] + "9" + n[4:])
    return v


marcas = []   # (contato_id, id da etiqueta no WhatsApp)
sem_contato = sem_numero = grupos = 0
for c in conversas:
    jid = c["jid"] or ""
    if jid.endswith("@g.us"):
        grupos += 1
        continue
    tel = jid.split("@")[0] if jid.endswith("@s.whatsapp.net") else (c.get("tel") or "").split("@")[0]
    if not tel:
        sem_numero += 1
        continue
    cid = next((contatos[v] for v in variantes(tel) if v in contatos), None)
    if cid is None:
        sem_contato += 1
        continue
    for lid in c["labels"]:
        if str(lid) in etiquetas:
            marcas.append((cid, str(lid)))

print("etiquetas de verdade no WhatsApp:", ", ".join(e["nome"] for e in etiquetas.values()))
print("conversas etiquetadas:", len(conversas), "| grupos:", grupos, "| sem telefone identificável:", sem_numero, "| sem conversa no CRM:", sem_contato)
for lid, e in etiquetas.items():
    print("  ", e["nome"], "→", len({m[0] for m in marcas if m[1] == lid}), "contatos do CRM")

if APLICAR:
    ids = {}
    for lid, e in etiquetas.items():
        ja = db.execute("SELECT id FROM etiquetas WHERE lower(nome) = lower(?)", (e["nome"].strip(),)).fetchone()
        if ja:
            ids[lid] = ja["id"]
        else:
            ids[lid] = db.execute("INSERT INTO etiquetas (nome, cor) VALUES (?, ?)", (e["nome"].strip(), CORES.get(str(e["cor"]), "#6f8f9c"))).lastrowid
    novas = 0
    for cid, lid in set(marcas):
        if not db.execute("SELECT 1 FROM contato_etiquetas WHERE contato_id = ? AND etiqueta_id = ?", (cid, ids[lid])).fetchone():
            db.execute("INSERT INTO contato_etiquetas (contato_id, etiqueta_id) VALUES (?, ?)", (cid, ids[lid]))
            novas += 1
    db.commit()
    print("APLICADO:", len(ids), "etiquetas no CRM;", novas, "marcações novas.")
    for r in db.execute("SELECT e.nome, e.cor, (SELECT COUNT(*) FROM contato_etiquetas ce WHERE ce.etiqueta_id = e.id) n FROM etiquetas e ORDER BY e.id"):
        print("  ", r["nome"], r["cor"], r["n"], "contatos")
else:
    print("SÓ MOSTREI (nada gravado)")
