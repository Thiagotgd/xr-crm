#!/usr/bin/env python3
"""Devolve o aviso de "não lida" às conversas que foram marcadas como lidas só porque um robô respondeu (Athena ou a
saudação automática), nas últimas HORAS horas. Roda no host. Sem "aplicar" só mostra.
Uma conversa entra se: a marca de leitura dela coincide (até 3 s) com uma resposta automática e, depois disso, ninguém
da recepção escreveu. A leitura volta para um segundo antes da primeira mensagem do cliente que ficou sem resposta
humana."""
import sqlite3
import sys

APLICAR = "aplicar" in sys.argv[1:]
nums = [a for a in sys.argv[1:] if a.isdigit()]
HORAS = int(nums[0]) if nums else 18
DB = "/docker/openclaw-cx8a/data/.openclaw/workspace/xr-crm/crm.db"
AUTO = ("%Assistente virtual Athena%", "%agradece seu contato%")

d = sqlite3.connect(DB, timeout=20)
d.row_factory = sqlite3.Row
candidatos = d.execute("""
SELECT c.id, c.last_read_at FROM contatos c
WHERE c.last_read_at >= datetime('now','localtime', ?)
  AND EXISTS (SELECT 1 FROM mensagens m WHERE m.contato_id = c.id AND m.direcao = 'out' AND (m.texto LIKE ? OR m.texto LIKE ?)
              AND abs(strftime('%s', m.created_at) - strftime('%s', c.last_read_at)) <= 3)
""", ("-%d hours" % HORAS,) + AUTO).fetchall()
mudar = []
for c in candidatos:
    # alguém da recepção escreveu depois da marca de leitura? então a conversa foi vista de verdade
    humano_depois = d.execute("SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at > ? AND texto NOT LIKE ? AND texto NOT LIKE ? LIMIT 1", (c["id"], c["last_read_at"]) + AUTO).fetchone()
    if humano_depois:
        continue
    # última mensagem de gente da recepção antes da resposta automática
    ult_humano = d.execute("SELECT MAX(created_at) FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at <= ? AND texto NOT LIKE ? AND texto NOT LIKE ?", (c["id"], c["last_read_at"]) + AUTO).fetchone()[0] or "1970-01-01 00:00:00"
    primeira = d.execute("SELECT MIN(created_at) FROM mensagens WHERE contato_id = ? AND direcao = 'in' AND created_at > ? AND created_at <= ?", (c["id"], ult_humano, c["last_read_at"])).fetchone()[0]
    if not primeira:
        continue
    mudar.append((c["id"], primeira))
print("conversas marcadas como lidas por resposta automática nas últimas %d h:" % HORAS, len(candidatos), "| sem resposta de gente depois (voltam a não lida):", len(mudar))
if APLICAR:
    for cid, primeira in mudar:
        d.execute("UPDATE contatos SET last_read_at = datetime(?, '-1 seconds') WHERE id = ?", (primeira, cid))
    d.commit()
    print("APLICADO:", len(mudar), "conversas voltaram a aparecer como não lidas")
else:
    print("SÓ MOSTREI (nada gravado)")
