'use strict';

// ─── Modo sombra do atendimento ────────────────────────────────────────────────
// Um agente de IA redige, EM SILÊNCIO, a resposta que daria a cada mensagem de
// cliente que a recepção já respondeu. Um segundo passo compara o rascunho com a
// resposta real e dá o veredito. O resultado é um placar por tipo de conversa:
// onde o agente já responderia igual à recepção, onde erra e do que ele sente falta
// (agenda, plano, PDF, anexos).
//
// NADA é enviado a cliente nenhum. O módulo só lê as conversas e grava na tabela
// sombra_turnos. A IA é chamada pela ponte da casa (xr-claude-bridge, assinatura),
// uma chamada por vez, só em horário comercial e com teto diário, para não disputar
// a ponte com a Athena (noite) nem com os outros apps.
//
// Desligar: variável de ambiente SOMBRA=0 no pm2 (ou REGRAS.ativo = false).

const fs = require('fs');
const path = require('path');

const REGRAS = {
  ativo: process.env.SOMBRA !== '0',
  porDia: 40,              // teto de conversas avaliadas por dia (2 chamadas à IA cada)
  porRodada: 3,            // quantas por rodada
  intervaloMin: 20,        // de quanto em quanto tempo roda
  horaIni: 9, horaFim: 18, // só em horário comercial (a Athena usa a ponte à noite)
  janelaHoras: 30,         // olha as conversas das últimas N horas
  maxEsperaRespostaH: 24,  // resposta da recepção depois disso não é "resposta àquela mensagem"
  blocoFechadoMin: 20,     // a resposta da recepção só é avaliada depois de N min sem nova mensagem dela
  historicoMsgs: 40,       // quantas mensagens anteriores o agente lê
  modeloRascunho: 'sonnet', // mesmo motor da Athena: o placar mede o agente que de fato responderia
  modeloJuiz: 'opus',       // quem compara é um modelo mais forte que quem redige
  prontaMinimo: 30,        // nº de conversas comparáveis para considerar uma categoria
  prontaTaxa: 95,          // % de respostas equivalentes para "pronta para autonomia"
};

const BRIDGE_URL = process.env.CLAUDE_BRIDGE_URL || 'http://172.18.0.1:18802/texto';
const ATHENA_MARCA = 'Assistente virtual Athena';
const SAUDACAO_MARCA = 'agradece seu contato';
const INTERNOS = ['553182043117', '553183283626', '553183746380', '553184505939'];

const ACOES = ['responder', 'enviar_orcamento_pdf', 'consultar_agenda', 'consultar_plano', 'ver_anexo', 'consultar_laudo', 'passar_para_humano', 'nao_responder'];
const VEREDITOS = ['equivalente', 'parcial', 'divergente', 'depende_de_sistema', 'nao_comparavel'];
const CATEGORIAS = {
  preco_exame_simples: 'Preço de exame simples (ultrassom, raio-X, eco, sangue)',
  orcamento_tc_rm: 'Orçamento de tomografia ou ressonância',
  sem_pedido: 'Cliente sem pedido do veterinário',
  agenda: 'Data, horário e agendamento',
  plano: 'Plano ou convênio',
  pre_anestesicos_preparo: 'Pré-anestésicos, preparo e jejum',
  pagamento: 'Pagamento, desconto e nota fiscal',
  laudo_resultado: 'Laudo, resultado e imagens',
  pos_agendamento: 'Confirmação, chegada e dia do exame',
  servico_nao_oferecido: 'Serviço que a XR não faz',
  urgencia_clinica: 'Urgência, dúvida clínica ou luto',
  triagem: 'Saudação e triagem (o que o cliente precisa)',
  nao_cliente: 'Não é cliente (fornecedor, currículo, parceiro, interno)',
  outro: 'Outro',
};

const dt = s => { if (!s) return null; const d = new Date(String(s).replace(' ', 'T')); return isNaN(d.getTime()) ? null : d; };
const p2 = n => String(n).padStart(2, '0');
const sql = d => `${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())} ${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}`;
const pct = (a, b) => b ? Math.round(1000 * a / b) / 10 : 0;
const DIAS = ['domingo', 'segunda-feira', 'terça-feira', 'quarta-feira', 'quinta-feira', 'sexta-feira', 'sábado'];

// ─── Banco ─────────────────────────────────────────────────────────────────────
function init(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS sombra_turnos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contato_id INTEGER NOT NULL,
    in_ini INTEGER, in_fim INTEGER NOT NULL UNIQUE, out_ini INTEGER, out_fim INTEGER,
    quando TEXT,                 -- hora da última mensagem do cliente no bloco
    cliente TEXT, real TEXT,     -- o que o cliente disse / o que a recepção respondeu
    acao TEXT, rascunho TEXT, motivo TEXT,          -- o que o agente faria
    categoria TEXT, veredito TEXT, erro_grave INTEGER DEFAULT 0, humano_necessario INTEGER DEFAULT 0, explicacao TEXT,
    modelo_rascunho TEXT, modelo_juiz TEXT, ms INTEGER, erro TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime'))
  )`);
  try { db.exec("CREATE INDEX IF NOT EXISTS idx_sombra_created ON sombra_turnos(created_at)"); } catch {}
}

// ─── Texto das mensagens ───────────────────────────────────────────────────────
function textoMsg(m) {
  const t = String(m.texto || '').trim();
  if (m.tipo === 'image') return '[imagem]' + (t && t !== '[image]' ? ' ' + t : '');
  if (m.tipo === 'audio') return '[áudio]';
  if (m.tipo === 'document') return '[documento: ' + (t && t !== '[document]' ? t : 'sem nome') + ']';
  return t;
}
// Foto ou PDF do cliente que o leitor (leitor.js) já transcreveu aparece com o conteúdo; senão, só o marcador.
function textoComLeitura(db, m) {
  if (m.direcao === 'in' && (m.tipo === 'image' || m.tipo === 'document' || m.tipo === 'audio')) {
    try { const t = require('./leitor').textoParaAgente(db, m.id); if (t) return t; } catch {}
  }
  return textoMsg(m);
}
const quem = m => m.direcao === 'in' ? 'CLIENTE' : String(m.texto || '').includes(ATHENA_MARCA) ? 'ATHENA' : 'RECEPÇÃO';
const ehAuto = m => m.direcao === 'out' && String(m.texto || '').includes(SAUDACAO_MARCA);
const ehAthena = m => m.direcao === 'out' && String(m.texto || '').includes(ATHENA_MARCA);
const limpaAthena = t => String(t).replace(/🐾\s*_?Assistente virtual Athena_?\s*🐾\s*/g, '').trim();

// ─── Achar os turnos (bloco do cliente → bloco de resposta humana) ─────────────
function acharTurnos(db, agora = new Date(), opts = {}) {
  const followup = require('./followup');
  const janelaH = opts.janelaHoras || REGRAS.janelaHoras;
  const desde = sql(new Date(agora.getTime() - janelaH * 3600000));
  const contatos = db.prepare(`SELECT DISTINCT m.contato_id AS id FROM mensagens m JOIN contatos c ON c.id = m.contato_id
    WHERE m.created_at >= ? AND m.direcao = 'in' AND COALESCE(c.followup_ignorar,0) = 0 AND COALESCE(c.equipe_xr,0) = 0`).all(desde)
    .map(r => r.id);
  const tel = db.prepare('SELECT telefone FROM contatos WHERE id = ?');
  const msgsDe = db.prepare("SELECT id, direcao, texto, tipo, created_at FROM mensagens WHERE contato_id = ? AND created_at >= ? ORDER BY id");
  const turnos = [];
  for (const id of contatos) {
    const t = tel.get(id);
    if (t && INTERNOS.includes(String(t.telefone).replace(/\D/g, ''))) continue;
    const msgs = msgsDe.all(id, desde).filter(m => !ehAuto(m));
    let i = 0;
    while (i < msgs.length) {
      if (msgs[i].direcao !== 'in') { i++; continue; }
      const ini = i;
      while (i < msgs.length && msgs[i].direcao === 'in') i++;
      const blocoIn = msgs.slice(ini, i);
      if (i >= msgs.length) break;                       // cliente falou e ninguém respondeu ainda
      if (ehAthena(msgs[i])) { while (i < msgs.length && msgs[i].direcao === 'out') i++; continue; } // quem respondeu foi a Athena
      const oIni = i;
      while (i < msgs.length && msgs[i].direcao === 'out' && !ehAthena(msgs[i])) i++;
      const blocoOut = msgs.slice(oIni, i);
      const ultCli = dt(blocoIn[blocoIn.length - 1].created_at);
      const priResp = dt(blocoOut[0].created_at), ultResp = dt(blocoOut[blocoOut.length - 1].created_at);
      if (!ultCli || !priResp || !ultResp) continue;
      if ((priResp - ultCli) > REGRAS.maxEsperaRespostaH * 3600000) continue;
      // bloco de resposta "fechado": veio nova mensagem do cliente depois, ou a recepção parou há N min
      const fechado = i < msgs.length || (agora - ultResp) >= REGRAS.blocoFechadoMin * 60000;
      if (!fechado) continue;
      // cortesia pura do cliente ("ok", "obrigada") não ensina nada
      const temMidia = blocoIn.some(m => m.tipo !== 'text');
      const txtCli = blocoIn.filter(m => m.tipo === 'text').map(m => String(m.texto || '')).join(' ');
      if (!temMidia && followup.ehFechamento(txtCli, txtCli.length)) continue;
      turnos.push({
        contato_id: id, in_ini: blocoIn[0].id, in_fim: blocoIn[blocoIn.length - 1].id, out_ini: blocoOut[0].id, out_fim: blocoOut[blocoOut.length - 1].id,
        quando: blocoIn[blocoIn.length - 1].created_at,
        cliente: blocoIn.map(m => textoComLeitura(db, m)).filter(Boolean).join('\n'),
        real: blocoOut.map(textoMsg).filter(Boolean).join('\n'),
      });
    }
  }
  return turnos;
}

// ─── Base de conhecimento e prompts ────────────────────────────────────────────
let _base = null;
function base() {
  if (_base && Date.now() - _base.em < 10 * 60000) return _base;
  let texto = '', tabela = '';
  try { texto = fs.readFileSync(path.join(__dirname, 'sombra-base.md'), 'utf8'); } catch (e) { texto = '(base de conhecimento não encontrada)'; }
  try {
    const lista = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'xr-orcamento', 'tabela-precos.json'), 'utf8'));
    tabela = lista.filter(x => x && x.nome && x.preco != null).map(x => `${x.nome}: R$ ${Number(x.preco).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`).join('\n');
  } catch (e) { tabela = ''; }
  _base = { texto, tabela, em: Date.now() };
  return _base;
}

const SYSTEM_RASCUNHO = `Você é atendente da recepção da XR Diagnóstico Veterinário respondendo clientes pelo WhatsApp. Isto é um TESTE: sua resposta NÃO será enviada; ela será comparada com a resposta que a recepção humana deu. Responda como responderia de verdade.

Regras:
- Siga estritamente a BASE DE CONHECIMENTO. Nunca invente preço, prazo, horário, cobertura de plano, preparo ou regra que não esteja nela.
- Tom de WhatsApp: cordial, direto, 1 a 4 frases curtas, em português, sem asteriscos e sem repetir o que já foi dito na conversa.
- Leia a conversa inteira antes: a mensagem pode ser continuação de um atendimento em andamento.
- Se a situação estiver em "O que o agente NÃO decide sozinho", use a ação passar_para_humano.
- Se precisar de algo que você não enxerga (agenda, plano, orçamento em PDF, conteúdo de imagem/áudio/documento, laudo), use a ação correspondente e escreva em "mensagem" o que diria ao cliente enquanto isso (pode ficar vazio).
- Se a mensagem não pede resposta, use nao_responder.

Ações possíveis: responder | enviar_orcamento_pdf | consultar_agenda | consultar_plano | ver_anexo | consultar_laudo | passar_para_humano | nao_responder

Saída: APENAS um objeto JSON, sem nenhum texto antes ou depois:
{"acao": "<uma das ações>", "mensagem": "<texto que enviaria ao cliente>", "motivo": "<por que, em uma frase curta>"}`;

const SYSTEM_JUIZ = `Você avalia, para a direção de uma clínica veterinária, se a resposta redigida por um agente de IA (RASCUNHO) equivale à resposta que a recepção humana realmente deu (REAL) à mesma mensagem do cliente. A REAL não é perfeita: julgue se o RASCUNHO levaria o cliente ao mesmo lugar, com informação correta segundo a BASE.

Vereditos:
- equivalente: mesma informação essencial e mesmo próximo passo. Palavras, ordem e tamanho diferentes não importam.
- parcial: mesmo rumo, mas falta ou sobra algo que muda a experiência do cliente (não ofereceu horário, não informou um valor que a REAL informou, pediu algo desnecessário).
- divergente: informação diferente ou outro próximo passo.
- depende_de_sistema: o RASCUNHO identificou corretamente o próximo passo, mas não pôde concluí-lo porque depende de agenda, plano, orçamento em PDF, anexo ou laudo que o agente não enxerga, e a REAL fez justamente esse passo.
- nao_comparavel: a REAL dependeu de fato fora da conversa (ligação, cliente presente, sistema interno), não responde à mensagem, ou o assunto não é atendimento a cliente.

erro_grave = true quando o RASCUNHO afirma preço, prazo, cobertura, preparo ou regra errados ou inventados, ou responde sozinho algo que deveria ter passado para uma pessoa (urgência, luto, dúvida clínica, negociação).
humano_necessario = true quando a situação exigia mesmo uma pessoa.

Categorias: ${Object.keys(CATEGORIAS).join(' | ')}

Saída: APENAS um objeto JSON, sem nenhum texto antes ou depois:
{"categoria": "<uma>", "veredito": "<um>", "erro_grave": true|false, "humano_necessario": true|false, "explicacao": "<até 200 caracteres, sem nomes de pessoas ou animais>"}`;

function historico(db, turno, limite) {
  const msgs = db.prepare("SELECT id, direcao, texto, tipo, created_at FROM mensagens WHERE contato_id = ? AND id <= ? ORDER BY id DESC LIMIT ?")
    .all(turno.contato_id, turno.in_fim, limite || REGRAS.historicoMsgs).reverse().filter(m => !ehAuto(m));
  return msgs.map(m => {
    const d = dt(m.created_at);
    const quando = d ? `${p2(d.getDate())}/${p2(d.getMonth() + 1)} ${p2(d.getHours())}:${p2(d.getMinutes())}` : '';
    let t = textoComLeitura(db, m); if (ehAthena(m)) t = limpaAthena(t);
    return `${quando} ${quem(m)}: ${t.replace(/\s*\n\s*/g, ' / ').slice(0, 900)}`;
  }).join('\n');
}

function promptRascunho(db, turno) {
  const b = base();
  const c = db.prepare("SELECT nome, pet_nome, etapa, orcamento_modalidade FROM contatos WHERE id = ?").get(turno.contato_id) || {};
  const d = dt(turno.quando) || new Date();
  const agora = `${DIAS[d.getDay()]}, ${p2(d.getDate())}/${p2(d.getMonth() + 1)}/${d.getFullYear()} às ${p2(d.getHours())}:${p2(d.getMinutes())}`;
  return `=== BASE DE CONHECIMENTO ===\n${b.texto}\n\n=== TABELA DE PREÇOS (particular) ===\n${b.tabela || '(indisponível)'}\n\n` +
    `=== AGORA ===\n${agora}\n\n=== CONTATO ===\nNome no WhatsApp: ${c.nome || '(sem nome)'}\nPet: ${c.pet_nome || '(não informado)'}\n` +
    `Etapa no CRM: ${c.etapa || '?'}${c.orcamento_modalidade ? ' · orçamento de ' + c.orcamento_modalidade : ''}\n\n` +
    `=== CONVERSA (mais antiga em cima) ===\n${historico(db, turno)}\n\n` +
    `Escreva o JSON com a sua resposta ao que o CLIENTE disse por último.`;
}

function promptJuiz(db, turno, rasc) {
  const b = base();
  const anterior = db.prepare("SELECT id, direcao, texto, tipo, created_at FROM mensagens WHERE contato_id = ? AND id < ? ORDER BY id DESC LIMIT 12")
    .all(turno.contato_id, turno.in_ini).reverse().filter(m => !ehAuto(m))
    .map(m => `${quem(m)}: ${(ehAthena(m) ? limpaAthena(textoMsg(m)) : textoComLeitura(db, m)).replace(/\s*\n\s*/g, ' / ').slice(0, 500)}`).join('\n');
  return `=== BASE (fatos válidos) ===\n${b.texto}\n\n=== CONTEXTO ANTERIOR ===\n${anterior || '(início da conversa)'}\n\n` +
    `=== MENSAGEM DO CLIENTE ===\n${turno.cliente.slice(0, 1500)}\n\n=== REAL (o que a recepção respondeu) ===\n${turno.real.slice(0, 2000)}\n\n` +
    `=== RASCUNHO (o que o agente faria) ===\nação: ${rasc.acao}\nmensagem: ${rasc.mensagem || '(nenhuma)'}\nmotivo: ${rasc.motivo || ''}\n\nDevolva o JSON da avaliação.`;
}

// ─── Chamada à ponte ───────────────────────────────────────────────────────────
function token() {
  for (const f of [path.join(__dirname, 'secrets', 'claude-bridge.key'), path.join(__dirname, '..', 'xr-claude-bridge', 'claude-bridge.key')]) {
    try { const t = fs.readFileSync(f, 'utf8').trim(); if (t) return t; } catch {}
  }
  return '';
}
async function chamarPonte(prompt, system, model) {
  const tk = token();
  if (!tk) throw new Error('token da ponte não encontrado');
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 170000);
  try {
    const r = await fetch(BRIDGE_URL, { method: 'POST', headers: { 'content-type': 'application/json', 'x-bridge-token': tk },
      body: JSON.stringify({ prompt, system, model, origem: 'xr-crm-sombra' }), signal: ctrl.signal });
    const j = await r.json().catch(() => null);
    if (!r.ok || !j || !j.ok) throw new Error('ponte: HTTP ' + r.status + ' ' + String((j && j.error) || '').slice(0, 160));
    return { texto: String(j.result || ''), modelo: j.model || model };
  } finally { clearTimeout(timer); }
}

function lerJson(texto) {
  const s = String(texto || '');
  const a = s.indexOf('{'), b = s.lastIndexOf('}');
  if (a < 0 || b <= a) return null;
  try { return JSON.parse(s.slice(a, b + 1)); } catch { return null; }
}

// ─── Avaliar um turno (rascunho → comparação) ──────────────────────────────────
async function avaliarTurno(db, turno, chamar = chamarPonte) {
  const t0 = Date.now();
  const linha = { ...turno, acao: '', rascunho: '', motivo: '', categoria: '', veredito: '', erro_grave: 0, humano_necessario: 0, explicacao: '', modelo_rascunho: '', modelo_juiz: '', ms: 0, erro: '' };
  try {
    const r1 = await chamar(promptRascunho(db, turno), SYSTEM_RASCUNHO, REGRAS.modeloRascunho);
    const rasc = lerJson(r1.texto);
    if (!rasc || !ACOES.includes(rasc.acao)) throw new Error('rascunho fora do formato: ' + r1.texto.slice(0, 120));
    linha.acao = rasc.acao; linha.rascunho = String(rasc.mensagem || '').slice(0, 3000); linha.motivo = String(rasc.motivo || '').slice(0, 300); linha.modelo_rascunho = r1.modelo;
    const r2 = await chamar(promptJuiz(db, turno, rasc), SYSTEM_JUIZ, REGRAS.modeloJuiz);
    const j = lerJson(r2.texto);
    if (!j || !VEREDITOS.includes(j.veredito)) throw new Error('avaliação fora do formato: ' + r2.texto.slice(0, 120));
    linha.categoria = CATEGORIAS[j.categoria] ? j.categoria : 'outro';
    linha.veredito = j.veredito; linha.erro_grave = j.erro_grave ? 1 : 0; linha.humano_necessario = j.humano_necessario ? 1 : 0;
    linha.explicacao = String(j.explicacao || '').slice(0, 400); linha.modelo_juiz = r2.modelo;
  } catch (e) { linha.erro = String(e.message || e).slice(0, 300); }
  linha.ms = Date.now() - t0;
  return linha;
}

function gravar(db, l) {
  db.prepare(`INSERT OR IGNORE INTO sombra_turnos (contato_id, in_ini, in_fim, out_ini, out_fim, quando, cliente, real, acao, rascunho, motivo,
      categoria, veredito, erro_grave, humano_necessario, explicacao, modelo_rascunho, modelo_juiz, ms, erro)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(l.contato_id, l.in_ini, l.in_fim, l.out_ini, l.out_fim, l.quando, l.cliente.slice(0, 3000), l.real.slice(0, 4000),
    l.acao, l.rascunho, l.motivo, l.categoria, l.veredito, l.erro_grave, l.humano_necessario, l.explicacao, l.modelo_rascunho, l.modelo_juiz, l.ms, l.erro);
}

// ─── Rodada ────────────────────────────────────────────────────────────────────
const pendentes = (db, turnos) => { const tem = db.prepare('SELECT 1 FROM sombra_turnos WHERE in_fim = ?'); return turnos.filter(t => !tem.get(t.in_fim)); };
const feitosHoje = db => db.prepare("SELECT COUNT(*) AS n FROM sombra_turnos WHERE created_at >= date('now','localtime')").get().n;
const noHorario = d => d.getDay() >= 1 && d.getDay() <= 6 && d.getHours() >= REGRAS.horaIni && d.getHours() < REGRAS.horaFim;

let rodando = false;
async function rodada(db, opts = {}) {
  if (rodando) return { pulou: 'já rodando' };
  const agora = opts.agora || new Date();
  if (!opts.forcar && (!REGRAS.ativo || !noHorario(agora))) return { pulou: 'fora do horário ou desligado' };
  const resta = REGRAS.porDia - feitosHoje(db);
  if (resta <= 0) return { pulou: 'teto do dia' };
  rodando = true;
  const feitos = [];
  try {
    const fila = pendentes(db, acharTurnos(db, agora));
    for (let i = fila.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [fila[i], fila[j]] = [fila[j], fila[i]]; }
    for (const turno of fila.slice(0, Math.min(opts.n || REGRAS.porRodada, resta))) {
      const l = await avaliarTurno(db, turno, opts.chamar);
      gravar(db, l);
      feitos.push(l);
      console.log(`[SOMBRA] contato ${l.contato_id}: ${l.erro ? 'ERRO ' + l.erro : l.categoria + ' → ' + l.veredito + (l.erro_grave ? ' (erro grave)' : '')} em ${l.ms} ms`);
    }
  } catch (e) { console.error('[SOMBRA] rodada:', e.message); }
  finally { rodando = false; }
  return { feitos: feitos.length, linhas: feitos };
}

function iniciar(db) {
  if (!REGRAS.ativo) { console.log('[SOMBRA] desligado (SOMBRA=0)'); return null; }
  console.log(`[SOMBRA] ligado: até ${REGRAS.porDia} conversas/dia, ${REGRAS.porRodada} a cada ${REGRAS.intervaloMin} min, ${REGRAS.horaIni}h-${REGRAS.horaFim}h. Nada é enviado a clientes.`);
  // primeira rodada pouco depois da subida (as seguintes, a cada intervalo)
  setTimeout(() => { rodada(db).catch(e => console.error('[SOMBRA]', e.message)); }, 90000);
  return setInterval(() => { rodada(db).catch(e => console.error('[SOMBRA]', e.message)); }, REGRAS.intervaloMin * 60000);
}

// ─── Placar ────────────────────────────────────────────────────────────────────
function resumo(db, dias) {
  dias = Math.max(1, Math.min(365, Number(dias) || 30));
  const desde = sql(new Date(Date.now() - dias * 86400000));
  const linhas = db.prepare("SELECT * FROM sombra_turnos WHERE created_at >= ? ORDER BY id DESC").all(desde);
  const ok = linhas.filter(l => !l.erro);
  const conta = (arr, v) => arr.filter(l => l.veredito === v).length;
  const bloco = arr => {
    const eq = conta(arr, 'equivalente'), pa = conta(arr, 'parcial'), di = conta(arr, 'divergente'), de = conta(arr, 'depende_de_sistema'), nc = conta(arr, 'nao_comparavel');
    const comp = eq + pa + di;
    const eg = arr.filter(l => l.erro_grave).length;
    return { n: arr.length, equivalente: eq, parcial: pa, divergente: di, depende_de_sistema: de, nao_comparavel: nc, comparaveis: comp,
      taxa: pct(eq, comp), erro_grave: eg, humano_necessario: arr.filter(l => l.humano_necessario).length,
      pronta: comp >= REGRAS.prontaMinimo && pct(eq, comp) >= REGRAS.prontaTaxa && eg === 0 };
  };
  const cats = {};
  for (const l of ok) (cats[l.categoria] = cats[l.categoria] || []).push(l);
  const acoes = {};
  for (const l of ok) acoes[l.acao] = (acoes[l.acao] || 0) + 1;
  const corta = (s, n) => { s = String(s || '').replace(/\s*\n\s*/g, ' / '); return s.length > n ? s.slice(0, n) + '…' : s; };
  return {
    periodo: { dias, desde: desde.slice(0, 10) },
    regras: { porDia: REGRAS.porDia, prontaMinimo: REGRAS.prontaMinimo, prontaTaxa: REGRAS.prontaTaxa, ativo: REGRAS.ativo },
    total: linhas.length, com_erro: linhas.length - ok.length,
    geral: bloco(ok),
    categorias: Object.entries(cats).map(([k, arr]) => ({ categoria: k, label: CATEGORIAS[k] || k, ...bloco(arr) })).sort((a, b) => b.n - a.n),
    acoes,
    exemplos: ok.filter(l => l.veredito === 'divergente' || l.erro_grave || l.veredito === 'parcial').slice(0, 15).map(l => ({
      id: l.id, contato_id: l.contato_id, quando: l.quando, categoria: CATEGORIAS[l.categoria] || l.categoria, veredito: l.veredito, erro_grave: !!l.erro_grave,
      cliente: corta(l.cliente, 260), real: corta(l.real, 320), acao: l.acao, rascunho: corta(l.rascunho, 320), explicacao: l.explicacao,
    })),
  };
}

module.exports = { REGRAS, CATEGORIAS, ACOES, VEREDITOS, init, acharTurnos, promptRascunho, promptJuiz, avaliarTurno, gravar, rodada, iniciar, resumo, lerJson, textoMsg, base, SYSTEM_RASCUNHO, SYSTEM_JUIZ };
