#!/usr/bin/env python3
"""Avisos do modo sombra pelo Telegram (bot XR Comercial, só para o administrador).

Roda no HOST, pelo cron, FORA do processo do CRM de propósito: se o CRM cair, o vigia ainda avisa.

  sombra-aviso.py semanal      → resumo da semana (segunda 8h15), para todos os aprovados no bot (Thiago e Carlos)
  sombra-aviso.py vigia        → só fala se o sombra parou ou está falhando (seg-sáb 18h20), só para o administrador
  sombra-aviso.py apresentacao → mensagem única explicando o que é o modo sombra (enviada em 11/11/2026)
  ... --dry-run                → imprime em vez de enviar

Lê o crm.db somente para leitura. Nenhum nome de cliente entra nas mensagens.
"""
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import urllib.parse
import urllib.request

AQUI = os.path.dirname(os.path.abspath(__file__))
CRM_DB = os.environ.get("CRM_DB", os.path.join(AQUI, "crm.db"))
RADAR = os.path.join(AQUI, "..", "xr-radar")
AJUSTES = os.path.join(AQUI, "sombra-ajustes.md")
MINIMO_DIA = 15          # abaixo disso num dia útil, o vigia avisa (o teto é 40)
PRONTA_MIN, PRONTA_TAXA = 30, 95

# Mesmos rótulos de sombra.js (CATEGORIAS)
ROTULO = {
    "preco_exame_simples": "Preço de exame simples",
    "orcamento_tc_rm": "Orçamento de TC ou RM",
    "sem_pedido": "Cliente sem pedido",
    "agenda": "Data e agendamento",
    "plano": "Plano ou convênio",
    "pre_anestesicos_preparo": "Pré-anestésicos e preparo",
    "pagamento": "Pagamento e nota",
    "laudo_resultado": "Laudo e resultado",
    "pos_agendamento": "Confirmação e dia do exame",
    "servico_nao_oferecido": "Serviço que a XR não faz",
    "urgencia_clinica": "Urgência, clínica ou luto",
    "triagem": "Saudação e triagem",
    "nao_cliente": "Não é cliente",
    "outro": "Outro",
}
ACAO = {"ver_anexo": "ler foto/PDF/áudio do cliente", "consultar_agenda": "ver a agenda", "consultar_plano": "conferir o plano",
        "enviar_orcamento_pdf": "gerar orçamento em PDF", "consultar_laudo": "ver o laudo"}


def banco():
    return sqlite3.connect(f"file:{CRM_DB}?mode=ro", uri=True, timeout=20)


def bloco(linhas):
    v = lambda x: sum(1 for l in linhas if l["veredito"] == x)
    eq, pa, di = v("equivalente"), v("parcial"), v("divergente")
    comp = eq + pa + di
    eg = sum(1 for l in linhas if l["erro_grave"])
    taxa = round(100 * eq / comp) if comp else 0
    return {"n": len(linhas), "eq": eq, "pa": pa, "di": di, "sis": v("depende_de_sistema"), "nc": v("nao_comparavel"), "comp": comp,
            "taxa": taxa, "eg": eg, "pronta": comp >= PRONTA_MIN and taxa >= PRONTA_TAXA and eg == 0}


def periodo(c, ini, fim):
    cur = c.execute("SELECT categoria, veredito, erro_grave, acao, erro FROM sombra_turnos WHERE created_at >= ? AND created_at < ?",
                    (ini.strftime("%Y-%m-%d %H:%M:%S"), fim.strftime("%Y-%m-%d %H:%M:%S")))
    todas = [dict(zip(("categoria", "veredito", "erro_grave", "acao", "erro"), r)) for r in cur]
    return [l for l in todas if not l["erro"]], [l for l in todas if l["erro"]]


def ajustes_da_semana(ini):
    try:
        linhas = open(AJUSTES, encoding="utf-8").read().splitlines()
    except OSError:
        return []
    out = []
    for l in linhas:
        m = re.match(r"^- (\d{4})-(\d{2})-(\d{2}): (.+)$", l.strip())
        if m and dt.datetime(int(m[1]), int(m[2]), int(m[3])) >= ini.replace(hour=0, minute=0, second=0, microsecond=0):
            out.append(f"• {m[3]}/{m[2]}: {m[4]}")
    return out


def texto_semanal(agora):
    ini, ant = agora - dt.timedelta(days=7), agora - dt.timedelta(days=14)
    with banco() as c:
        ok, falhas = periodo(c, ini, agora)
        ok_ant, _ = periodo(c, ant, ini)
    g, ga = bloco(ok), bloco(ok_ant)
    f = lambda d: d.strftime("%d/%m")
    t = [f"🤖 Modo sombra do atendimento — {f(ini)} a {f(agora)}", ""]
    if not ok:
        t += ["Nenhuma conversa avaliada nesta semana. O sombra pode estar parado: vale conferir.", ""]
        return "\n".join(t)
    t.append(f"Conversas avaliadas: {g['n']}" + (f" (semana anterior: {ga['n']})" if ga["n"] else "") + (f" · {len(falhas)} com falha técnica" if falhas else ""))
    if g["comp"]:
        seta = ""
        if ga["comp"] >= 20:
            d = g["taxa"] - ga["taxa"]
            seta = f" (▲ +{d} pontos)" if d > 0 else f" (▼ {d} pontos)" if d < 0 else " (igual à semana anterior)"
        t.append(f"Responderia igual à recepção: {g['taxa']}% ({g['eq']} de {g['comp']} comparáveis){seta}")
    t.append(f"Parcial: {g['pa']} · Diferente: {g['di']} · Travou por falta de sistema: {g['sis']} · Erro grave: {g['eg']}")
    t.append("")
    cats = {}
    for l in ok:
        cats.setdefault(l["categoria"] or "outro", []).append(l)
    linhas = sorted(((k, bloco(v)) for k, v in cats.items()), key=lambda x: -x[1]["n"])
    t.append("Por assunto (responderia igual / comparáveis):")
    for k, b in linhas[:8]:
        extra = f", {b['sis']} sem sistema" if b["sis"] else ""
        erro = f", {b['eg']} erro grave" if b["eg"] else ""
        marca = " ✅" if b["pronta"] else ""
        if b["comp"]:
            t.append(f"• {ROTULO.get(k, k)}: {b['taxa']}% ({b['eq']}/{b['comp']}){extra}{erro}{marca}")
        else:
            t.append(f"• {ROTULO.get(k, k)}: {b['n']} conversa{'s' if b['n'] != 1 else ''}, nenhuma comparável{extra}")
    prontas = [ROTULO.get(k, k) for k, b in linhas if b["pronta"]]
    t.append("")
    t.append("✅ Prontos para o próximo passo: " + (", ".join(prontas) if prontas else "nenhum ainda"))
    trava = {}
    for l in ok:
        if l["veredito"] == "depende_de_sistema" and l["acao"] in ACAO:
            trava[l["acao"]] = trava.get(l["acao"], 0) + 1
    if trava:
        t.append("Onde trava por falta de sistema: " + " · ".join(f"{ACAO[a]} {n}" for a, n in sorted(trava.items(), key=lambda x: -x[1])))
    aj = ajustes_da_semana(ini)
    t.append("")
    t += (["Ajustes na base nesta semana:"] + aj) if aj else ["Nenhum ajuste na base de conhecimento nesta semana."]
    t.append("")
    t.append(f'"Pronto" = {PRONTA_TAXA}% igual em pelo menos {PRONTA_MIN} conversas comparáveis, sem erro grave. Casos lado a lado: aba Métricas do CRM. Nada é enviado a clientes.')
    return "\n".join(t)


def texto_vigia(agora):
    """Devolve o alerta, ou '' se está tudo bem."""
    if agora.weekday() == 6:
        return ""
    try:
        with banco() as c:
            hoje = agora.strftime("%Y-%m-%d 00:00:00")
            n, erros = c.execute("SELECT COUNT(*), COALESCE(SUM(erro <> ''), 0) FROM sombra_turnos WHERE created_at >= ?", (hoje,)).fetchone()
            ult = c.execute("SELECT erro FROM sombra_turnos WHERE created_at >= ? AND erro <> '' ORDER BY id DESC LIMIT 1", (hoje,)).fetchone()
    except Exception as e:
        return f"⚠️ Modo sombra: não consegui ler o banco do CRM ({str(e)[:120]}). O CRM pode estar fora do ar."
    if n < MINIMO_DIA:
        return (f"⚠️ Modo sombra: hoje foram avaliadas só {n} conversas (o normal é perto de 40). "
                "Pode ter parado: ponte da IA, token ou o próprio CRM." + (f"\nÚltima falha: {ult[0][:160]}" if ult else ""))
    if erros * 2 >= n:
        return f"⚠️ Modo sombra: {erros} de {n} conversas de hoje deram falha técnica." + (f"\nÚltima falha: {ult[0][:160]}" if ult else "")
    return ""


def telegram(so_admin):
    """Bot XR Comercial (xr-radar). Resumo semanal: todos os aprovados (Thiago e Carlos). Vigia: só o administrador."""
    token = ""
    for linha in open(os.path.join(RADAR, ".env"), encoding="utf-8"):
        if linha.startswith("TELEGRAM_TOKEN="):
            token = linha.split("=", 1)[1].strip().strip('"').strip("'")
    c = sqlite3.connect(f"file:{os.path.join(RADAR, 'radar.db')}?mode=ro", uri=True, timeout=20)
    chats = [r[0] for r in c.execute("SELECT chat_id FROM usuarios WHERE admin = 1" if so_admin else "SELECT chat_id FROM usuarios WHERE aprovado = 1")]
    c.close()
    if not token or not chats:
        raise SystemExit("sem token do bot ou sem destinatário no xr-radar")
    return token, chats


APRESENTACAO = ("🤖 Aqui é o Robô Ti, assistente do Thiago na XR.\n\n"
                "Começou hoje o modo sombra do atendimento. É um teste em que uma IA redige, em silêncio, a resposta que daria a conversas "
                "que a recepção já respondeu no WhatsApp. Depois comparo com o que a recepção de fato respondeu, para medir em números "
                "o quanto ela já acertaria e em quais assuntos. Nada é enviado a clientes e nada muda no atendimento.\n\n"
                "Toda segunda às 8h15 eu mando aqui o resumo da semana: quantas conversas foram avaliadas, em quantas a IA responderia "
                "igual à recepção, onde ela erra e o que falta para ela evoluir. O primeiro chega na próxima segunda.")


def enviar(texto, so_admin):
    token, chats = telegram(so_admin)
    for chat in chats:
        for i in range(0, len(texto), 3900):
            data = urllib.parse.urlencode({"chat_id": chat, "text": texto[i:i + 3900], "disable_web_page_preview": "true"}).encode()
            with urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data, timeout=60) as r:
                if not json.load(r).get("ok"):
                    raise SystemExit("Telegram recusou a mensagem")
    return len(chats)


def main():
    modo = next((a for a in sys.argv[1:] if not a.startswith("--")), "")
    dry = "--dry-run" in sys.argv
    agora = dt.datetime.now()
    if modo == "semanal":
        texto = texto_semanal(agora)
    elif modo == "apresentacao":
        texto = APRESENTACAO
    elif modo == "vigia":
        texto = texto_vigia(agora)
        if not texto:
            print(agora.strftime("%d/%m %H:%M"), "vigia: tudo certo, nada enviado")
            return
    else:
        raise SystemExit(__doc__)
    if dry:
        print(texto)
        return
    n = enviar(texto, so_admin=(modo == "vigia"))
    print(agora.strftime("%d/%m %H:%M"), modo, "enviado para", n, "destinatário(s)")


if __name__ == "__main__":
    main()
