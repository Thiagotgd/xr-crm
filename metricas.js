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
  // Fora da conta: quem o motor reconheceu como laudo (exame já feito) e quem foi
  // devolvido de Orçamento para Agendado na correção em lote de 21/08/2026 16:10
  // (PDF era laudo). Sem isso, "agendados" infla com 64 correções que não são venda.
  const laudoIds = new Set([
    ...db.prepare("SELECT DISTINCT contato_id FROM followup_eventos WHERE tipo = 'encerrado' AND valor LIKE 'exame já feito%'").all().map(r => r.contato_id),
    ...db.prepare("SELECT DISTINCT contato_id FROM etapa_historico WHERE etapa_de = 'orcamento' AND etapa_para = 'agendado' AND created_at BETWEEN '2026-08-21 16:10:00' AND '2026-08-21 16:11:00'").all().map(r => r.contato_id),
  ]);
  const internos = ['553182043117', '553183283626', '553183746380', '553184505939'];
  const cohorte = db.prepare(`
    SELECT id, nome, telefone, etapa, orcamento_enviado_at AS orc, orcamento_modalidade AS mod
    FROM contatos WHERE orcamento_enviado_at >= ?
  `).all(desde).filter(c => !laudoIds.has(c.id) && !internos.includes(String(c.telefone).replace(/\D/g, '')));

  // "Fechou" = a mensagem de agendamento saiu (mesmas palavras que movem pra Agendado no webhook)
  // OU a recepção marcou pela tela/WhatsApp. O etapa_historico sozinho mistura arrasto manual e correções.
  const msgAgendado = db.prepare(`SELECT MIN(created_at) AS t FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at >= ?
    AND texto NOT LIKE '%${followup.ATHENA_MARCA}%'
    AND (LOWER(texto) LIKE '%exame agendado%' OR LOWER(texto) LIKE '%exame marcado%' OR LOWER(texto) LIKE '%agendamos%'
      OR LOWER(texto) LIKE '%agendado para%' OR LOWER(texto) LIKE '%agendada para%' OR LOWER(texto) LIKE '%marcado para%'
      OR LOWER(texto) LIKE '%marcada para%' OR LOWER(texto) LIKE '%confirmado para%' OR LOWER(texto) LIKE '%confirmada para%')`);
  const cmdMarcou = db.prepare("SELECT MIN(created_at) AS t FROM followup_eventos WHERE contato_id = ? AND tipo = 'comando' AND valor IN ('marcou','fechou','agendou') AND created_at >= ?");
  const histAgendadoDepois = { get: (id, orc) => { const a = msgAgendado.get(id, orc), b = cmdMarcou.get(id, orc); const ts = [a && a.t, b && b.t].filter(Boolean).sort(); return { t: ts[0] || null }; } };
  // "Respondeu": o cliente mandou qualquer mensagem depois do orçamento (engajou).
  // (O questionário pré-anestésico foi auditado: vai junto do PDF em 97% dos casos — é template, não aceite.)
  const clienteRespondeu = db.prepare("SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'in' AND created_at > ? LIMIT 1");
  const motivoEnc = db.prepare("SELECT valor FROM followup_eventos WHERE contato_id = ? AND tipo IN ('encerrado','comando') ORDER BY id DESC LIMIT 1");

  let responderam = 0, agendados = 0, perdidos = 0, abertos = 0;
  const diasFechar = [];
  const porMod = {};
  const motivos = {};

  for (const c of cohorte) {
    const ag = histAgendadoDepois.get(c.id, c.orc);
    const fechou = !!(ag && ag.t);
    if (fechou || clienteRespondeu.get(c.id, c.orc)) responderam++;

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

    // Modalidade: SÓ do nome do PDF (desde 21/08/2026). A estimativa por texto foi
    // auditada e errava 6 em 10 (o template de horário cita TC e RM) — removida.
    const k = c.mod || 'sem registro (PDF anterior a 21/08)';
    porMod[k] = porMod[k] || { orcados: 0, agendados: 0, perdidos: 0, abertos: 0 };
    porMod[k].orcados++;
    porMod[k][status === 'agendado' ? 'agendados' : status === 'perdido' ? 'perdidos' : 'abertos']++;
  }

  const modalidades = Object.entries(porMod)
    .map(([mod, v]) => ({ mod, ...v, taxa: pct(v.agendados, v.orcados), taxa_decididos: pct(v.agendados, v.agendados + v.perdidos) }))
    .sort((a, b) => b.orcados - a.orcados);

  // ── Tempo de resposta ao cliente (minutos úteis até a 1ª resposta humana) ──
  // Só conversas com clientes (fora números internos). Athena e a saudação automática
  // do WhatsApp Business não contam como resposta humana (auditado: 10% das "respostas" eram a saudação).
  const msgs = db.prepare("SELECT m.contato_id, m.direcao, m.created_at, SUBSTR(m.texto,1,60) AS t, c.telefone FROM mensagens m JOIN contatos c ON c.id = m.contato_id WHERE m.created_at >= ? ORDER BY m.contato_id, m.id").all(desde)
    .filter(m => !internos.includes(String(m.telefone).replace(/\D/g, '')));
  const esperas = [];
  let aguardandoDesde = null, ultimoContato = null;
  for (const m of msgs) {
    if (m.contato_id !== ultimoContato) { ultimoContato = m.contato_id; aguardandoDesde = null; }
    if (m.direcao === 'in') { if (!aguardandoDesde) aguardandoDesde = dt(m.created_at); }
    else if (aguardandoDesde && !String(m.t || '').includes(followup.ATHENA_MARCA) && !String(m.t || '').includes(followup.SAUDACAO_MARCA)) {
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
      orcados: cohorte.length, responderam, agendados, perdidos, abertos,
      taxa_fechamento: pct(agendados, cohorte.length),
      taxa_decididos: pct(agendados, agendados + perdidos),
      taxa_resposta: pct(responderam, cohorte.length),
    },
    tempo_fechar_dias: { mediana: diasFechar.length ? Math.round(mediana(diasFechar) * 10) / 10 : null, amostras: diasFechar.length },
    modalidades, modalidades_estimadas: 0,
    resposta,
    followup: { cobrados: cobrados.length, recuperados, taxa_recuperacao: pct(recuperados, cobrados.length), urgentes_no_periodo: urgentesResolvidos },
    motivos_perda: Object.entries(motivos).map(([motivo, n]) => ({ motivo, n })).sort((a, b) => b.n - a.n),
    por_dia: porDia,
  };
}

module.exports = { calcular };
