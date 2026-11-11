#!/usr/bin/env python3
"""Testa as rotas novas na CÓPIA DE ENSAIO do CRM (porta 8196, WhatsApp de mentira). Compara a lista com a produção (8095)."""
import gzip
import json
import sqlite3
import time
import urllib.error
import urllib.request

ENS = "http://172.18.0.3:8196"
PROD = "http://172.18.0.3:8095"
DBE = "/docker/openclaw-cx8a/data/.openclaw/workspace/_ensaio-crm/crm.db"
ok = falhou = 0


def confere(nome, cond, extra=""):
    global ok, falhou
    if cond:
        ok += 1
        print("  ok   ", nome, extra)
    else:
        falhou += 1
        print("  FALHA", nome, extra)


def req(base, path, method="GET", body=None, headers=None):
    h = {"Accept-Encoding": "gzip"}
    h.update(headers or {})
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    r = urllib.request.Request(base + path, data=data, method=method, headers=h)
    t0 = time.time()
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            raw = resp.read()
            st, hd = resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        raw, st, hd = e.read(), e.code, dict(e.headers)
    ms = int((time.time() - t0) * 1000)
    fio = len(raw)
    if hd.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    try:
        j = json.loads(raw.decode() or "null")
    except Exception:
        j = None
    return st, hd, j, ms, fio, len(raw)


print("== lista de conversas: produção (antes) x ensaio (depois) ==")
st, hd, velha, ms, fio, tam = req(PROD, "/api/contatos/conversas")
print(f"  produção: {len(velha)} contatos, {tam // 1024} KB, {ms} ms")
st, hd, nova, ms2, fio2, tam2 = req(ENS, "/api/contatos/conversas")
print(f"  ensaio  : {len(nova)} contatos, {tam2 // 1024} KB ({fio2 // 1024} KB no fio, compactado), {ms2} ms")
confere("lista veio compactada e com ETag", hd.get("Content-Encoding") == "gzip" and bool(hd.get("ETag")))
confere("lista nova é menor", tam2 < tam / 2, f"{tam2 * 100 // tam}% do tamanho antigo")
st3, hd3, j3, ms3, fio3, _ = req(ENS, "/api/contatos/conversas", headers={"If-None-Match": hd["ETag"]})
confere("nada mudou -> 304 sem corpo", st3 == 304 and fio3 == 0, f"{ms3} ms")
# mesmas conversas no topo, mesmos não lidos
topo_v = [c["id"] for c in velha[:60]]
topo_n = [c["id"] for c in nova[:60]]
iguais = sum(1 for a, b in zip(topo_v, topo_n) if a == b)
print(f"  topo da lista igual em {iguais} de 60 posições (o banco do ensaio é uma cópia de minutos atrás)")
mv = {c["id"]: c for c in velha}
dif = [c["id"] for c in nova if c["id"] in mv and (c["nao_lidas"] != mv[c["id"]]["nao_lidas"] and abs(c["nao_lidas"] - mv[c["id"]]["nao_lidas"]) > 3)]
confere("contagem de não lidas bate com a produção (tolerância de mensagens novas)", len(dif) <= 3, f"diferentes: {len(dif)}")
nl_v = sum(1 for c in velha if c["nao_lidas"] > 0 or c["manual_unread"])
nl_n = sum(1 for c in nova if c["nao_lidas"] > 0 or c["manual_unread"])
print(f"  conversas com não lidas: produção {nl_v} | ensaio {nl_n} (as que faltam são não lidas com mais de 45 dias)")
fora = [c for c in velha if c["id"] not in {x["id"] for x in nova}]
print(f"  ficaram fora da lista padrão: {len(fora)} contatos; o mais recente deles falou em {max((c.get('last_message_at') or '') for c in fora)[:10] if fora else '-'}")
campos = set(nova[0].keys())
for f in ("id", "nome", "telefone", "etapa", "foto_url", "pinado", "favorito", "manual_unread", "nao_lidas", "ultima_msg", "ultima_msg_dir", "ultima_msg_at", "ultima_msg_tipo", "etiquetas", "orcamento_enviado_at", "ultima_resposta_cliente_at", "last_message_at", "created_at"):
    if f not in campos:
        confere("campo " + f, False)
st, hd, tudo, ms, _, tamt = req(ENS, "/api/contatos/conversas?tudo=1")
confere("?tudo=1 traz todos", len(tudo) == len(velha) or abs(len(tudo) - len(velha)) < 5, f"{len(tudo)} x {len(velha)}")
# busca em todos os contatos
if fora:
    alvo = next((c for c in fora if len((c.get("nome") or "").split(" ")[0]) >= 4), fora[0])
    termo = (alvo.get("nome") or "").split(" ")[0][:6].lower() or alvo["telefone"][-6:]
    st, hd, ach, ms, _, _ = req(ENS, "/api/contatos/conversas?q=" + urllib.request.quote(termo))
    confere("busca acha contato antigo que saiu da lista", any(c["id"] == alvo["id"] for c in ach) or len(ach) == 80, f"{len(ach)} resultados em {ms} ms")
    st, hd, ach, ms, _, _ = req(ENS, "/api/contatos/conversas?q=" + alvo["telefone"][-7:])
    confere("busca por final do telefone", any(c["id"] == alvo["id"] for c in ach), f"{len(ach)} resultados")

print("\n== conversa: 50 mais recentes, anteriores, lido/não lido ==")
db = sqlite3.connect(DBE)
db.row_factory = sqlite3.Row
cid = db.execute("select contato_id from mensagens group by contato_id having count(*) > 130 and sum(direcao='in') > 20 order by max(id) desc limit 1").fetchone()[0]
total = db.execute("select count(*) from mensagens where contato_id=?", (cid,)).fetchone()[0]
# deixa o contato como "não lido" para testar o visto=0
db.execute("update contatos set last_read_at='2020-01-01 00:00:00', manual_unread=0 where id=?", (cid,))
db.commit()
open("/tmp/evo-falsa-marca", "w").write(str(time.time()))
st, hd, c0, ms, _, _ = req(ENS, f"/api/contato/{cid}?visto=0")
lr = db.execute("select last_read_at from contatos where id=?", (cid,)).fetchone()[0]
confere("visto=0 NÃO marca como lido", lr == "2020-01-01 00:00:00", f"last_read_at={lr}")
confere("vieram 50 e avisa que há mais", len(c0["mensagens"]) == 50 and c0["tem_mais"] is True, f"total no banco {total}")
log_antes = open("/tmp/evo-copia.log").read() if False else ""
st, hd, c1, ms, _, _ = req(ENS, f"/api/contato/{cid}")
lr = db.execute("select last_read_at from contatos where id=?", (cid,)).fetchone()[0]
confere("abrir normal marca como lido", lr > "2026-01-01", f"last_read_at={lr}")
# paginação até o fim, sem repetir e sem pular
ids = [m["id"] for m in c1["mensagens"]]
tem, voltas = c1["tem_mais"], 0
while tem and voltas < 60:
    st, hd, p, ms, _, _ = req(ENS, f"/api/contato/{cid}/mensagens?antes={ids[-1]}")
    ids += [m["id"] for m in p["mensagens"]]
    tem = p["tem_mais"]
    voltas += 1
confere("anteriores: traz todas, sem repetir", len(ids) == total and len(set(ids)) == total, f"{len(ids)} de {total} em {voltas} cargas")
lr2 = db.execute("select last_read_at from contatos where id=?", (cid,)).fetchone()[0]
confere("carregar anteriores não mexe no lido", lr2 == lr)
st, hd, p, ms, _, _ = req(ENS, f"/api/contato/{cid}/mensagens")
confere("anteriores sem referência -> erro 400", st == 400)

print("\n== envio (vai para o WhatsApp de mentira) ==")
alvo_in = db.execute("select id, texto, wa_msg_id from mensagens where contato_id=? and direcao='in' and wa_msg_id!='' and tipo='text' order by id desc limit 1", (cid,)).fetchone()
st, hd, r, ms, _, _ = req(ENS, f"/api/contato/{cid}/mensagem", "POST", {"texto": "linha 1\nlinha 2\n\nlinha 4"})
ult = db.execute("select * from mensagens where contato_id=? order by id desc limit 1", (cid,)).fetchone()
confere("texto com quebras de linha gravado igual", st == 200 and ult["texto"] == "linha 1\nlinha 2\n\nlinha 4" and ult["wa_msg_id"].startswith("ENSAIO"))
st, hd, r, ms, _, _ = req(ENS, f"/api/contato/{cid}/mensagem", "POST", {"texto": "resposta citando", "citar": alvo_in["id"]})
ult = db.execute("select * from mensagens where contato_id=? order by id desc limit 1", (cid,)).fetchone()
confere("resposta citando grava o trecho e de quem era", st == 200 and r.get("sem_citacao") is False and ult["citacao_texto"] == (alvo_in["texto"] or "")[:300] and ult["citacao_de"] == "in")
sem_id = db.execute("select id from mensagens where contato_id=? and wa_msg_id='' order by id desc limit 1", (cid,)).fetchone()
if sem_id:
    st, hd, r, ms, _, _ = req(ENS, f"/api/contato/{cid}/mensagem", "POST", {"texto": "tentando citar antiga", "citar": sem_id[0]})
    ult = db.execute("select * from mensagens where contato_id=? order by id desc limit 1", (cid,)).fetchone()
    confere("mensagem sem identificador: envia sem citar e avisa", st == 200 and r.get("sem_citacao") is True and ult["citacao_texto"] == "")
else:
    print("  (sem mensagem antiga sem identificador para testar)")
antes = db.execute("select count(*) from mensagens where contato_id=?", (cid,)).fetchone()[0]
st, hd, r, ms, _, _ = req(ENS, f"/api/contato/{cid}/mensagem", "POST", {"texto": "FALHAR de propósito"})
depois = db.execute("select count(*) from mensagens where contato_id=?", (cid,)).fetchone()[0]
confere("WhatsApp recusou -> erro na tela e NADA gravado", st == 500 and depois == antes, str((r or {}).get("error"))[:90])
png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
st, hd, r, ms, _, _ = req(ENS, f"/api/contato/{cid}/media", "POST", {"base64": png, "mediatype": "image", "mimetype": "image/png", "fileName": "imagem.png", "caption": ""})
ult = db.execute("select * from mensagens where contato_id=? order by id desc limit 1", (cid,)).fetchone()
confere("foto sem legenda: não mostra nome do arquivo e guarda o identificador", st == 200 and ult["texto"] == "[image]" and ult["tipo"] == "image" and ult["wa_msg_id"].startswith("ENSAIO"))
st, hd, r, ms, _, _ = req(ENS, f"/api/contato/{cid}/media", "POST", {"base64": png, "mediatype": "document", "mimetype": "application/pdf", "fileName": "Orçamento Teste.pdf", "caption": ""})
ult = db.execute("select * from mensagens where contato_id=? order by id desc limit 1", (cid,)).fetchone()
confere("documento guarda o nome do arquivo", st == 200 and ult["texto"] == "Orçamento Teste.pdf" and ult["tipo"] == "document")

print("\n== webhook: mensagem do cliente respondendo a uma nossa ==")
tel = db.execute("select telefone from contatos where id=?", (cid,)).fetchone()[0]
nossa = db.execute("select wa_msg_id, texto from mensagens where contato_id=? and direcao='out' and wa_msg_id like 'ENSAIO%' and tipo='text' order by id asc limit 1", (cid,)).fetchone()
evento = {"event": "messages.upsert", "data": {"key": {"remoteJid": tel + "@s.whatsapp.net", "fromMe": False, "id": "TESTEIN" + str(int(time.time()))},
          "pushName": "Teste", "message": {"extendedTextMessage": {"text": "sim, pode ser", "contextInfo": {"stanzaId": nossa["wa_msg_id"], "participant": "5531999990000@s.whatsapp.net", "quotedMessage": {"conversation": nossa["texto"]}}}}}}
st, hd, r, ms, _, _ = req(ENS, "/webhook/evolution", "POST", evento)
ult = db.execute("select * from mensagens where contato_id=? order by id desc limit 1", (cid,)).fetchone()
confere("resposta do cliente guarda o que ele citou (mensagem nossa)", st == 200 and ult["texto"] == "sim, pode ser" and ult["citacao_texto"] == nossa["texto"][:300] and ult["citacao_de"] == "out")
evento2 = {"event": "messages.upsert", "data": {"key": {"remoteJid": tel + "@s.whatsapp.net", "fromMe": False, "id": "TESTEIN2" + str(int(time.time()))},
           "pushName": "Teste", "message": {"conversation": "mensagem comum"}}}
st, hd, r, ms, _, _ = req(ENS, "/webhook/evolution", "POST", evento2)
ult = db.execute("select * from mensagens where contato_id=? order by id desc limit 1", (cid,)).fetchone()
confere("mensagem comum continua sem citação", ult["texto"] == "mensagem comum" and ult["citacao_texto"] == "")
st, hd, c2, ms, _, _ = req(ENS, f"/api/contato/{cid}?visto=0")
confere("conversa devolve os campos de citação", any(m.get("citacao_texto") for m in c2["mensagens"]))
print("\ncontato usado no ensaio:", cid)
print(f"\nRESULTADO: {ok} ok, {falhou} falha(s)")
