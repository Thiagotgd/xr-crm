#!/usr/bin/env python3
"""Testa, na cópia de ensaio do CRM (porta 8196): pausar e religar a Athena em um contato e a consulta que a
Athena faz antes de responder."""
import datetime
import json
import sqlite3
import urllib.error
import urllib.request

ENS = "http://172.18.0.3:8196"
DB = "/docker/openclaw-cx8a/data/.openclaw/workspace/_ensaio-crm/crm.db"
ok = falhou = 0
TEL = "5500000000001"


def confere(nome, cond, extra=""):
    global ok, falhou
    if cond:
        ok += 1
        print("  ok   ", nome, extra)
    else:
        falhou += 1
        print("  FALHA", nome, extra)


def req(path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(ENS + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None


def banco(sql, *a):
    d = sqlite3.connect(DB, timeout=10)
    d.row_factory = sqlite3.Row
    cur = d.execute(sql, a)
    r = [dict(x) for x in cur] if cur.description else []
    d.commit()
    d.close()
    return r


def pausada(numero):
    return req("/api/athena/pausada?numero=" + numero)[1]


agora = datetime.datetime.utcnow() - datetime.timedelta(hours=3)  # horário de Brasília
cid = banco("SELECT id FROM contatos WHERE telefone = ?", TEL)[0]["id"]

st, c = req("/api/contato/%d" % cid)
confere("contato novo vem com a Athena normal", st == 200 and c.get("athena_pausada") is False, str(c.get("athena_pausada")))
confere("consulta da Athena: não pausado", pausada(TEL) == {"pausada": False, "ate": ""}, str(pausada(TEL)))

st, j = req("/api/contato/%d/athena" % cid, "POST", {"modo": "volta"})
ate = datetime.datetime.strptime(j["athena_pausa_ate"], "%Y-%m-%d %H:%M:%S")
confere("pausar até a recepção abrir: fica pausada", st == 200 and j["athena_pausada"] is True, j["athena_pausa_ate"])
confere("  volta às 7h30", ate.hour == 7 and ate.minute == 30)
confere("  de um dia de segunda a sábado", ate.weekday() != 6, ate.strftime("%A"))
confere("  no futuro e em no máximo 2 dias", agora < ate <= agora + datetime.timedelta(days=2, hours=1), str(ate - agora))
confere("consulta da Athena: pausado", pausada(TEL).get("pausada") is True and pausada(TEL).get("ate") == j["athena_pausa_ate"], str(pausada(TEL)))
st, c = req("/api/contato/%d" % cid)
confere("a conversa abre mostrando a pausa", c.get("athena_pausada") is True and c.get("athena_pausa_ate") == j["athena_pausa_ate"])

st, j = req("/api/contato/%d/athena" % cid, "POST", {"modo": "ligar"})
confere("religar tira a pausa", st == 200 and j["athena_pausada"] is False and j["athena_pausa_ate"] == "")
confere("consulta da Athena: liberado de novo", pausada(TEL).get("pausada") is False)

st, j = req("/api/contato/%d/athena" % cid, "POST", {"modo": "24h"})
ate = datetime.datetime.strptime(j["athena_pausa_ate"], "%Y-%m-%d %H:%M:%S")
confere("pausar por 24 horas", j["athena_pausada"] is True and abs((ate - agora).total_seconds() - 86400) < 120, j["athena_pausa_ate"])

st, j = req("/api/contato/%d/athena" % cid, "POST", {"modo": "sempre"})
confere("pausar até religar", j["athena_pausada"] is True and j["athena_pausa_ate"].startswith("9999"), j["athena_pausa_ate"])
confere("consulta da Athena: pausado", pausada(TEL).get("pausada") is True)

banco("UPDATE contatos SET athena_pausa_ate = '2020-01-01 07:30:00' WHERE id = ?", cid)
confere("pausa vencida: a Athena volta sozinha", pausada(TEL).get("pausada") is False)
st, c = req("/api/contato/%d" % cid)
confere("  e a conversa abre sem a pausa", c.get("athena_pausada") is False)

# o mesmo número com e sem o nono dígito
banco("INSERT INTO contatos (nome, telefone, etapa, athena_pausa_ate) VALUES ('ENSAIO Sem Nove', '553188887777', 'novo', '9999-12-31 00:00:00')")
banco("INSERT INTO contatos (nome, telefone, etapa, athena_pausa_ate) VALUES ('ENSAIO Com Nove', '5531966665555', 'novo', '9999-12-31 00:00:00')")
confere("número gravado sem o 9, WhatsApp manda com o 9", pausada("5531988887777").get("pausada") is True)
confere("número gravado com o 9, WhatsApp manda sem o 9", pausada("553166665555").get("pausada") is True)
confere("número igual ao gravado", pausada("553188887777").get("pausada") is True)
confere("endereço completo do WhatsApp também serve", pausada("553188887777@s.whatsapp.net").get("pausada") is True)

confere("número que não está no CRM: não pausado", pausada("5531900001111").get("pausada") is False)
confere("sem número: não pausado", req("/api/athena/pausada")[1].get("pausada") is False)
outro = banco("SELECT telefone FROM contatos WHERE COALESCE(athena_pausa_ate,'') = '' AND length(telefone) >= 12 AND telefone NOT LIKE '%@%' LIMIT 1")[0]["telefone"]
confere("outro contato qualquer continua com a Athena", pausada(outro).get("pausada") is False)
confere("nenhum contato real foi pausado", banco("SELECT COUNT(*) n FROM contatos WHERE COALESCE(athena_pausa_ate,'') != '' AND nome NOT LIKE 'ENSAIO%'")[0]["n"] == 0)

st, j = req("/api/contato/%d/athena" % cid, "POST", {"modo": "xyz"})
confere("modo desconhecido é recusado", st == 400, str(st))
st, j = req("/api/contato/99999999/athena", "POST", {"modo": "volta"})
confere("contato que não existe", st == 404, str(st))
# deixa o contato de ensaio pausado para a conferência da tela e do lado da Athena
req("/api/contato/%d/athena" % cid, "POST", {"modo": "volta"})
print("\nRESULTADO: %d ok, %d falha(s) | contato de ensaio=%d (ficou pausado)" % (ok, falhou, cid))
