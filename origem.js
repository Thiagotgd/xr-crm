'use strict';

// ─── Origem do contato (de onde o cliente veio) ────────────────────────────────
// Detectada pelas primeiras mensagens que o CLIENTE manda. Nada é perguntado a
// ninguém e nada é enviado: é só leitura do que já cai no espelho do WhatsApp.
//
//   meta    → clicou num anúncio da Meta (Instagram/Facebook). O anúncio preenche
//             um texto pronto ("Olá! Posso ter mais informações sobre isso?").
//   google  → veio do botão de WhatsApp do perfil da XR no Google
//             ("Olá, vim do Google e gostaria de mais informações").
//   direto  → tudo o mais (indicação do vet, número salvo, boca a boca…).
//
// `origem_detalhe` guarda o texto pronto que casou, para quando a agência usar
// uma frase diferente por anúncio: aí dá para saber QUAL peça trouxe o cliente.

const ORIGENS = {
  meta:   { label: 'Anúncio Meta', curto: 'Anúncio', emoji: '📣', cor: '#1877f2' },
  google: { label: 'Google',       curto: 'Google',  emoji: '🔎', cor: '#34a853' },
  direto: { label: 'Direto / indicação', curto: 'Direto', emoji: '',  cor: '#6b7280' },
};

// Quantas mensagens do cliente olhamos antes de cravar "direto".
// (Às vezes a 1ª é um "Bom dia" e a frase pronta vem logo depois.)
const JANELA_MSGS = 3;

const normalizar = s => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();

// Padrões em texto normalizado (sem acento, minúsculo).
const PADROES = [
  // Meta — texto pronto padrão dos anúncios click-to-WhatsApp da XR + variações
  // que a agência pode configurar por anúncio ("vi o anúncio sobre ressonância").
  { origem: 'meta',   re: /posso ter mais informacoes sobre isso/ },
  { origem: 'meta',   re: /gostaria de obter mais informacoes/ },
  { origem: 'meta',   re: /\bvi (o |seu |esse |este |um )?anuncio\b/ },
  { origem: 'meta',   re: /\b(vim|vi|cheguei) (pelo|do|no|pela|da) (instagram|insta|facebook|face)\b/ },
  // Google — botão de WhatsApp do Perfil da Empresa
  { origem: 'google', re: /\bvim do google\b/ },
  { origem: 'google', re: /\b(vi|achei|encontrei) (voces |vcs )?(no|pelo) google\b/ },
];

function detectar(texto) {
  const t = normalizar(texto);
  if (!t) return null;
  for (const p of PADROES) if (p.re.test(t)) return p.origem;
  return null;
}

function init(db) {
  try { db.exec("ALTER TABLE contatos ADD COLUMN origem TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN origem_detalhe TEXT DEFAULT ''"); } catch {}
}

// Chamado pelo webhook a cada mensagem RECEBIDA (direcao 'in'), depois de gravá-la.
// Regra: enquanto o contato tiver até JANELA_MSGS mensagens recebidas, uma frase
// de anúncio/Google sobrescreve "direto". Depois disso a origem está cravada.
function classificar(db, contatoId, texto) {
  const c = db.prepare('SELECT origem FROM contatos WHERE id = ?').get(contatoId);
  if (!c) return null;
  if (c.origem && c.origem !== 'direto') return c.origem; // já é paga/Google: não mexe
  const n = db.prepare("SELECT COUNT(*) AS n FROM mensagens WHERE contato_id = ? AND direcao = 'in'").get(contatoId).n;
  if (n > JANELA_MSGS && c.origem) return c.origem; // janela fechada
  const det = detectar(texto);
  if (det) {
    db.prepare("UPDATE contatos SET origem = ?, origem_detalhe = ? WHERE id = ?").run(det, String(texto || '').trim().slice(0, 120), contatoId);
    console.log(`[ORIGEM] contato ${contatoId} → ${det}`);
    return det;
  }
  if (!c.origem) {
    db.prepare("UPDATE contatos SET origem = 'direto' WHERE id = ?").run(contatoId);
    return 'direto';
  }
  return c.origem;
}

// Uma vez na subida: classifica quem ainda não tem origem pelas primeiras mensagens.
// Idempotente — quem já tem origem não é tocado.
function backfill(db) {
  const pendentes = db.prepare("SELECT id FROM contatos WHERE COALESCE(origem,'') = ''").all();
  if (!pendentes.length) return 0;
  const primeiras = db.prepare("SELECT texto FROM mensagens WHERE contato_id = ? AND direcao = 'in' ORDER BY id LIMIT ?");
  const upd = db.prepare("UPDATE contatos SET origem = ?, origem_detalhe = ? WHERE id = ?");
  const cont = { meta: 0, google: 0, direto: 0 };
  db.transaction(() => {
    for (const c of pendentes) {
      let origem = 'direto', detalhe = '';
      for (const m of primeiras.all(c.id, JANELA_MSGS)) {
        const d = detectar(m.texto);
        if (d) { origem = d; detalhe = String(m.texto || '').trim().slice(0, 120); break; }
      }
      upd.run(origem, detalhe, c.id);
      cont[origem]++;
    }
  })();
  console.log(`[ORIGEM] backfill: ${pendentes.length} contatos → meta ${cont.meta} · google ${cont.google} · direto ${cont.direto}`);
  return pendentes.length;
}

// ─── Resumo por origem (para a aba Métricas e o relatório da Dani) ─────────────
// Cohorte = contatos CRIADOS no período (quem chegou), por origem.
// "agendou" usa as mesmas palavras do webhook/métricas ("Exame agendado" etc.)
// ou o comando marcou/fechou da recepção. Números internos ficam fora.
const sqlDate = d => { const p = n => String(n).padStart(2, '0'); return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`; };
const pct = (a, b) => b ? Math.round(1000 * a / b) / 10 : 0;
const INTERNOS = ['553182043117', '553183283626', '553183746380', '553184505939'];

// ─── Região do cliente (pelo DDD do telefone) ──────────────────────────────────
// Serve para ver até onde a XR alcança (e se um anúncio aberto para o interior
// trouxe gente de lá). Limites: o DDD 31 junta BH, Contagem, Sete Lagoas, Itabira,
// Ouro Preto e Ipatinga; e DDD é onde o chip foi comprado, não onde a pessoa mora.
const REGIOES = {
  bh:     { label: 'BH e entorno (31)', ordem: 1 },
  d37:    { label: 'Divinópolis e centro-oeste (37)', ordem: 2 },
  d32:    { label: 'Juiz de Fora e Zona da Mata (32)', ordem: 3 },
  d33:    { label: 'Gov. Valadares e leste (33)', ordem: 4 },
  d38:    { label: 'Montes Claros e norte (38)', ordem: 5 },
  d34:    { label: 'Triângulo Mineiro (34)', ordem: 6 },
  d35:    { label: 'Sul de Minas (35)', ordem: 7 },
  sp:     { label: 'São Paulo (estado)', ordem: 8 },
  rj:     { label: 'Rio de Janeiro (estado)', ordem: 9 },
  es:     { label: 'Espírito Santo', ordem: 10 },
  outros: { label: 'Outros estados', ordem: 11 },
  semddd: { label: 'Sem DDD (exterior ou número oculto)', ordem: 12 },
};
const DDD_REGIAO = { 31: 'bh', 37: 'd37', 32: 'd32', 33: 'd33', 38: 'd38', 34: 'd34', 35: 'd35', 21: 'rj', 22: 'rj', 24: 'rj', 27: 'es', 28: 'es' };
for (let d = 11; d <= 19; d++) DDD_REGIAO[d] = 'sp';

// O CRM guarda o telefone como vem do WhatsApp: 55 + DDD + número (12 ou 13 dígitos).
// Qualquer outra coisa (exterior, identificador oculto do WhatsApp) fica "sem DDD".
function regiaoDoTelefone(tel) {
  const t = String(tel || '').replace(/\D/g, '');
  if (!t.startsWith('55') || (t.length !== 12 && t.length !== 13)) return 'semddd';
  const ddd = Number(t.slice(2, 4));
  if (ddd < 11) return 'semddd';
  return DDD_REGIAO[ddd] || 'outros';
}

function resumo(db, dias, athenaMarca) {
  dias = Math.max(1, Math.min(365, Number(dias) || 30));
  const agora = new Date();
  const desde = `${sqlDate(new Date(agora.getTime() - dias * 86400000))} 00:00:00`;
  const marca = '%' + (athenaMarca || 'Assistente virtual Athena') + '%';

  const contatos = db.prepare(`
    SELECT id, telefone, COALESCE(NULLIF(origem,''),'direto') AS origem, origem_detalhe, orcamento_enviado_at,
      COALESCE(orcamento_modalidade,'') AS modalidade
    FROM contatos WHERE created_at >= ? AND COALESCE(followup_ignorar, 0) = 0
  `).all(desde).filter(c => !INTERNOS.includes(String(c.telefone).replace(/\D/g, '')));

  const agendou = db.prepare(`SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND texto NOT LIKE ?
    AND (LOWER(texto) LIKE '%exame agendado%' OR LOWER(texto) LIKE '%exames agendados%' OR LOWER(texto) LIKE '%exame marcado%' OR LOWER(texto) LIKE '%exames marcados%' OR LOWER(texto) LIKE '%agendamos%'
      OR LOWER(texto) LIKE '%agendado para%' OR LOWER(texto) LIKE '%agendada para%' OR LOWER(texto) LIKE '%marcado para%'
      OR LOWER(texto) LIKE '%marcada para%' OR LOWER(texto) LIKE '%confirmado para%' OR LOWER(texto) LIKE '%confirmada para%') LIMIT 1`);
  const cmdMarcou = db.prepare("SELECT 1 FROM followup_eventos WHERE contato_id = ? AND ((tipo = 'comando' AND valor IN ('marcou','fechou','agendou')) OR (tipo = 'encerrado' AND valor LIKE 'agendado (confirma%')) LIMIT 1");
  const respondeu = db.prepare("SELECT COUNT(*) AS n FROM mensagens WHERE contato_id = ? AND direcao = 'in'");

  const por = {};
  const detalhes = {};
  const reg = {};
  for (const k of Object.keys(ORIGENS)) por[k] = { origem: k, ...ORIGENS[k], contatos: 0, conversaram: 0, orcados: 0, agendados: 0 };
  for (const c of contatos) {
    const o = por[c.origem] || (por[c.origem] = { origem: c.origem, label: c.origem, curto: c.origem, emoji: '', cor: '#6b7280', contatos: 0, conversaram: 0, orcados: 0, agendados: 0 });
    o.contatos++;
    // "conversou" = mandou ao menos 2 mensagens (não só o texto pronto do anúncio)
    if (respondeu.get(c.id).n >= 2) o.conversaram++;
    if (c.orcamento_enviado_at) o.orcados++;
    const ag = !!(agendou.get(c.id, marca) || cmdMarcou.get(c.id));
    if (ag) o.agendados++;
    if (c.origem !== 'direto') {
      const k = c.origem + '|' + (c.origem_detalhe || '');
      detalhes[k] = detalhes[k] || { origem: c.origem, detalhe: c.origem_detalhe || '', contatos: 0, agendados: 0 };
      detalhes[k].contatos++;
      if (ag) detalhes[k].agendados++;
    }
    // Região (DDD): quem chegou, quem pediu exame avançado (TC/RM), quem agendou, e por qual origem.
    const rk = regiaoDoTelefone(c.telefone);
    const r = reg[rk] || (reg[rk] = { regiao: rk, label: REGIOES[rk].label, ordem: REGIOES[rk].ordem, contatos: 0, orcados: 0, tcrm: 0, agendados: 0, meta: 0, google: 0, direto: 0 });
    r.contatos++;
    if (c.orcamento_enviado_at) r.orcados++;
    if (c.modalidade === 'TC' || c.modalidade === 'RM') r.tcrm++;
    if (ag) r.agendados++;
    if (r[c.origem] !== undefined) r[c.origem]++;
  }
  const origens = Object.values(por).map(o => ({ ...o, taxa: pct(o.agendados, o.contatos) }))
    .sort((a, b) => (a.origem === 'direto') - (b.origem === 'direto') || b.contatos - a.contatos);
  const regioes = Object.values(reg)
    .map(r => ({ ...r, taxa: pct(r.agendados, r.contatos), pct_contatos: pct(r.contatos, contatos.length) }))
    .sort((a, b) => a.ordem - b.ordem);
  const foraBh = regioes.filter(r => r.regiao !== 'bh' && r.regiao !== 'semddd');
  const soma = (arr, k) => arr.reduce((s, r) => s + r[k], 0);
  return {
    periodo: { dias, desde: desde.slice(0, 10), ate: sqlDate(agora) },
    total: contatos.length,
    origens,
    detalhes: Object.values(detalhes).sort((a, b) => b.contatos - a.contatos),
    regioes,
    // Resumo "fora de BH e entorno": quanto da demanda (e do exame avançado) já vem de longe.
    fora_bh: {
      contatos: soma(foraBh, 'contatos'), pct_contatos: pct(soma(foraBh, 'contatos'), contatos.length),
      tcrm: soma(foraBh, 'tcrm'), pct_tcrm: pct(soma(foraBh, 'tcrm'), soma(regioes, 'tcrm')),
      agendados: soma(foraBh, 'agendados'),
    },
  };
}

module.exports = { ORIGENS, PADROES, JANELA_MSGS, REGIOES, detectar, regiaoDoTelefone, init, classificar, backfill, resumo };
