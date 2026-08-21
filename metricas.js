'use strict';

// ─── Métricas de vendas / marcações ────────────────────────────────────────────
// Tudo calculado do que o CRM já grava. Somente leitura.
// Cohorte = contatos que receberam orçamento no período (orcamento_enviado_at),
// excluindo os que o motor reconheceu como laudo (exame já feito).

const followup = require('./followup');

const dt = s => { if (!s) return null; const d = new Date(String(s).replace(' ', 'T')); return isNaN(d.getTime()) ? null : d; };
const sqlDate = d => { const p = n => String(n).padStart(2, '0'); return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`; };
const mediana = arr => { if (!arr.length) return null; const a = [...arr].sort((x, y) => x - y); const m = Math.floor(a.length / 2); return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2; };
const pct = (a, b) => b ? Math.round(1000 * a / b) / 10 : 0;

function calcular(db, dias) {
  dias = Math.max(1, Math.min(365, Number(dias) || 30));
  const agora = new Date();
  const desdeD = new Date(agora.getTime() - dias * 86400000);
  const desde = `${sqlDate(desdeD)} 00:00:00`;

  // ── Cohorte ────────────────────────────────────────────────────────────────
  const laudoIds = new Set(db.prepare("SELECT DISTINCT contato_id FROM followup_eventos WHERE tipo = 'encerrado' AND valor LIKE 'exame já feito%'").all().map(r => r.contato_id));
  const internos = ['553182043117', '553183283626', '553183746380', '553184505939'];
  const cohorte = db.prepare(`
    SELECT id, nome, telefone, etapa, orcamento_enviado_at AS orc, orcamento_modalidade AS mod
    FROM contatos WHERE orcamento_enviado_at >= ?
  `).all(desde).filter(c => !laudoIds.has(c.id) && !internos.includes(String(c.telefone).replace(/\D/g, '')));

  const histAgendadoDepois = db.prepare("SELECT MIN(created_at) AS t FROM etapa_historico WHERE contato_id = ? AND etapa_para = 'agendado' AND created_at >= ?");
  const temPreAnest = db.prepare("SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at >= ? AND (LOWER(texto) LIKE '%pré-anestésico%' OR LOWER(texto) LIKE '%pre-anestesico%' OR LOWER(texto) LIKE '%pré anestésico%' OR LOWER(texto) LIKE '%pre anestesico%') LIMIT 1");
  const textoPerto = db.prepare("SELECT LOWER(texto) AS t FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at BETWEEN datetime(?, '-2 days') AND datetime(?, '+2 days')");
  const motivoEnc = db.prepare("SELECT valor FROM followup_eventos WHERE contato_id = ? AND tipo IN ('encerrado','comando') ORDER BY id DESC LIMIT 1");

  let aceitos = 0, agendados = 0, perdidos = 0, abertos = 0;
  const diasFechar = [];
  const porMod = {};
  const motivos = {};
  let modEstimados = 0;

  for (const c of cohorte) {
    const ag = histAgendadoDepois.get(c.id, c.orc);
    const fechou = !!(ag && ag.t);
    const aceitou = fechou || !!temPreAnest.get(c.id, c.orc);
    if (aceitou) aceitos++;

    let status;
    if (fechou) {
      status = 'agendado'; agendados++;
      const d0 = dt(c.orc), d1 = dt(ag.t);
      if (d0 && d1) diasFechar.push((d1 - d0) / 86400000);
    } else if (c.etapa === 'finalizado') {
      status = 'perdido'; perdidos++;
      const m = motivoEnc.get(c.id);
      const v = m ? String(m.valor) : '';
      const chave = /luto/.test(v) ? 'luto (animal faleceu)' : /desist|perdeu/.test(v) ? 'desistiu' : /ok/.test(v) ? 'resolvido por fora' : 'sumiu / sem resposta';
      motivos[chave] = (motivos[chave] || 0) + 1;
    } else { status = 'aberto'; abertos++; }

    // Modalidade: do nome do PDF; se vazio, estima pelo que o time escreveu perto do orçamento.
    let mod = c.mod || '';
    if (!mod) {
      const ts = textoPerto.all(c.id, c.orc, c.orc).map(r => r.t).join(' ');
      const tc = /tomograf/.test(ts), rm = /resson/.test(ts);
      mod = tc && rm ? 'TC+RM' : tc ? 'TC' : rm ? 'RM' : '';
      if (mod) modEstimados++;
    }
    const k = mod || 'não identificada';
    porMod[k] = porMod[k] || { orcados: 0, agendados: 0, perdidos: 0, abertos: 0 };
    porMod[k].orcados++;
    porMod[k][status === 'agendado' ? 'agendados' : status === 'perdido' ? 'perdidos' : 'abertos']++;
  }

  const modalidades = Object.entries(porMod)
    .map(([mod, v]) => ({ mod, ...v, taxa: pct(v.agendados, v.orcados), taxa_decididos: pct(v.agendados, v.agendados + v.perdidos) }))
    .sort((a, b) => b.orcados - a.orcados);

  // ── Tempo de resposta ao cliente (minutos úteis até a 1ª resposta humana) ──
  const msgs = db.prepare("SELECT contato_id, direcao, created_at, SUBSTR(texto,1,40) AS t FROM mensagens WHERE created_at >= ? ORDER BY contato_id, id").all(desde);
  const esperas = [];
  let aguardandoDesde = null, ultimoContato = null;
  for (const m of msgs) {
    if (m.contato_id !== ultimoContato) { ultimoContato = m.contato_id; aguardandoDesde = null; }
    if (m.direcao === 'in') { if (!aguardandoDesde) aguardandoDesde = dt(m.created_at); }
    else if (aguardandoDesde && !String(m.t || '').includes(followup.ATHENA_MARCA)) {
      const fim = dt(m.created_at);
      if (fim) esperas.push(followup.minutosUteisEntre(aguardandoDesde, fim));
      aguardandoDesde = null;
    }
  }
  const resposta = {
    amostras: esperas.length,
    mediana_min: esperas.length ? Math.round(mediana(esperas)) : null,
    p90_min: esperas.length ? Math.round([...esperas].sort((a, b) => a - b)[Math.floor(esperas.length * 0.9)]) : null,
    pct_ate_30min: esperas.length ? pct(esperas.filter(x => x <= 30).length, esperas.length) : null,
    pct_ate_2h: esperas.length ? pct(esperas.filter(x => x <= 120).length, esperas.length) : null,
  };

  // ── Follow-up: cobrados e recuperados ───────────────────────────────────────
  const cobrados = db.prepare("SELECT contato_id, MIN(created_at) AS t FROM followup_eventos WHERE tipo = 'nivel' AND valor IN ('2','3') AND created_at >= ? GROUP BY contato_id").all(desde);
  let recuperados = 0;
  for (const r of cobrados) { const ag = histAgendadoDepois.get(r.contato_id, r.t); if (ag && ag.t) recuperados++; }
  const urgentesResolvidos = db.prepare("SELECT COUNT(DISTINCT contato_id) AS c FROM followup_eventos WHERE tipo = 'nivel' AND valor = '1' AND created_at >= ?").get(desde).c;

  // ── Por dia ─────────────────────────────────────────────────────────────────
  const porDiaMap = {};
  for (let i = dias - 1; i >= 0; i--) { const d = new Date(agora.getTime() - i * 86400000); porDiaMap[sqlDate(d)] = { dia: sqlDate(d), orcados: 0, agendados: 0 }; }
  for (const r of db.prepare("SELECT SUBSTR(orcamento_enviado_at,1,10) AS d, COUNT(*) AS c FROM contatos WHERE orcamento_enviado_at >= ? GROUP BY d").all(desde)) if (porDiaMap[r.d]) porDiaMap[r.d].orcados = r.c;
  for (const r of db.prepare("SELECT SUBSTR(created_at,1,10) AS d, COUNT(DISTINCT contato_id) AS c FROM etapa_historico WHERE etapa_para = 'agendado' AND created_at >= ? GROUP BY d").all(desde)) if (porDiaMap[r.d]) porDiaMap[r.d].agendados = r.c;
  const porDia = Object.values(porDiaMap);

  return {
    periodo: { dias, desde: desde.slice(0, 10), ate: sqlDate(agora) },
    funil: {
      orcados: cohorte.length, aceitos, agendados, perdidos, abertos,
      taxa_fechamento: pct(agendados, cohorte.length),
      taxa_decididos: pct(agendados, agendados + perdidos),
      taxa_aceite: pct(aceitos, cohorte.length),
    },
    tempo_fechar_dias: { mediana: diasFechar.length ? Math.round(mediana(diasFechar) * 10) / 10 : null, amostras: diasFechar.length },
    modalidades, modalidades_estimadas: modEstimados,
    resposta,
    followup: { cobrados: cobrados.length, recuperados, taxa_recuperacao: pct(recuperados, cobrados.length), urgentes_no_periodo: urgentesResolvidos },
    motivos_perda: Object.entries(motivos).map(([motivo, n]) => ({ motivo, n })).sort((a, b) => b.n - a.n),
    por_dia: porDia,
  };
}

module.exports = { calcular };
