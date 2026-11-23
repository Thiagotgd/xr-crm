'use strict';
// ─── Conexão do WhatsApp da recepção (22/11/2026) ──────────────────────────────────────────────────────────────
// Depois de dois dias com o espelho quebrado (19-21/11/2026: a Evolution perdeu a ligação e o WhatsApp deslogou o
// aparelho), o CRM ganhou:
//   1. estado da ligação (aberta / fechada / conectando), com a última mensagem recebida;
//   2. o QR code (ou o código por número) para religar, direto na engrenagem do CRM;
//   3. a SINCRONIZAÇÃO do que ficou de fora: ao reconectar, o WhatsApp manda o histórico recente ("messages.set"
//      pelo webhook da Evolution). Cada mensagem que o CRM ainda não tem é gravada com a data/hora verdadeira, na
//      ordem certa. Conversa em que a recepção respondeu pelo celular fica lida até a resposta; o que o cliente
//      mandou depois da última resposta continua NÃO LIDO (regra do Thiago: o que não foi aberto precisa aparecer).
//      A Evolution desta VPS não guarda o histórico (DATABASE_SAVE_DATA_HISTORIC=false), por isso tem de ser na hora.
// Não envia nada a ninguém. Não mexe na Evolution além de pedir o QR.
const http = require('http');

const JANELA_DIAS = Number(process.env.HISTORICO_DIAS || 14); // histórico mais velho que isso é ignorado

let cfg = null;
let processar = null; // processarMensagem(data, event, opcoes) do server.js
const fila = [];
let rodando = false;

const sync = {
  emAndamento: false, lotes: 0, recebidas: 0, gravadas: 0, ignoradas: 0, foraDaJanela: 0, erros: 0,
  contatos: new Set(), inicio: '', fim: '', progresso: null, ultimoLote: '', ultimoErro: '',
  desde: '', ate: '', lidas: 0, naoLidas: 0
};
let conexao = { estado: '', motivo: '', quando: '' }; // último connection.update visto pelo webhook

function agoraTexto() {
  return new Date().toLocaleString('sv-SE', { timeZone: 'America/Sao_Paulo' }).replace('T', ' ').slice(0, 19);
}
function textoDoTimestamp(ts) {
  const n = Number(ts);
  if (!n) return '';
  return new Date(n * 1000).toLocaleString('sv-SE', { timeZone: 'America/Sao_Paulo' }).replace('T', ' ').slice(0, 19);
}

function iniciar(opcoes) {
  cfg = { base: opcoes.evoBase, key: opcoes.evoKey, instancia: opcoes.instancia, db: opcoes.db, marcas: opcoes.marcas || [] };
  processar = opcoes.processarMensagem;
}

function evoGet(caminho) {
  return new Promise((resolve, reject) => {
    const url = new URL(cfg.base + caminho);
    const req = http.request({ hostname: url.hostname, port: url.port, path: url.pathname + url.search, method: 'GET', headers: { 'apikey': cfg.key } }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { let j = null; try { j = JSON.parse(d); } catch {} resolve({ status: res.statusCode, json: j, texto: d }); });
    });
    req.on('error', reject);
    req.setTimeout(25000, () => { req.destroy(); reject(new Error('a Evolution demorou a responder')); });
    req.end();
  });
}

// Estado da instância na Evolution: open | close | connecting | ? (Evolution fora do ar)
async function consultarEstado() {
  try {
    const r = await evoGet(`/instance/connectionState/${cfg.instancia}`);
    const st = r.json && r.json.instance && r.json.instance.state;
    return { estado: st || '?', http: r.status };
  } catch (e) {
    return { estado: '?', erro: e.message };
  }
}

// Quando foi desligado e por quê (a Evolution guarda na lista de instâncias)
async function detalhesInstancia() {
  try {
    const r = await evoGet('/instance/fetchInstances');
    const lista = Array.isArray(r.json) ? r.json : [];
    const i = lista.find(x => x && (x.name === cfg.instancia || (x.instance && x.instance.instanceName === cfg.instancia)));
    if (!i) return {};
    return { desconectouEm: i.disconnectionAt || '', motivo: i.disconnectionReasonCode || '', numero: String(i.ownerJid || '').split('@')[0] };
  } catch { return {}; }
}

function resumoSync() {
  return {
    emAndamento: sync.emAndamento, lotes: sync.lotes, recebidas: sync.recebidas, gravadas: sync.gravadas, ignoradas: sync.ignoradas,
    foraDaJanela: sync.foraDaJanela, erros: sync.erros, conversas: sync.contatos.size, inicio: sync.inicio, fim: sync.fim,
    progresso: sync.progresso, ultimoLote: sync.ultimoLote, naFila: fila.reduce((s, l) => s + l.lista.length, 0), ultimoErro: sync.ultimoErro,
    desde: sync.desde, ate: sync.ate, lidas: sync.lidas, naoLidas: sync.naoLidas
  };
}

async function estado() {
  const st = await consultarEstado();
  const det = st.estado === 'open' ? {} : await detalhesInstancia();
  let ultimaRecebida = '', ultimaEnviada = '';
  try {
    ultimaRecebida = (cfg.db.prepare("SELECT MAX(created_at) m FROM mensagens WHERE direcao = 'in'").get() || {}).m || '';
    ultimaEnviada = (cfg.db.prepare("SELECT MAX(created_at) m FROM mensagens WHERE direcao = 'out'").get() || {}).m || '';
  } catch {}
  return { instancia: cfg.instancia, estado: st.estado, evolution: st.erro ? 'fora do ar: ' + st.erro : 'ok', ...det, ultimaRecebida, ultimaEnviada, conexao, sync: resumoSync(), agora: agoraTexto() };
}

// Pede o QR (ou o código por número) à Evolution. Se a instância já está aberta, diz isso.
async function gerarQr(numero) {
  const st = await consultarEstado();
  if (st.estado === 'open') return { conectado: true, estado: 'open' };
  const q = numero ? `?number=${encodeURIComponent(String(numero).replace(/\D/g, ''))}` : '';
  const r = await evoGet(`/instance/connect/${cfg.instancia}${q}`);
  if (r.status >= 400 || !r.json) return { erro: `a Evolution não deu o QR (código ${r.status})`, detalhe: String(r.texto || '').slice(0, 200) };
  const j = r.json;
  if (j.instance && j.instance.state === 'open') return { conectado: true, estado: 'open' };
  return { conectado: false, estado: st.estado, base64: j.base64 || '', code: j.code || '', pairingCode: j.pairingCode || '', count: j.count || 0 };
}

// ─── Histórico que o WhatsApp manda ao reconectar ──────────────────────────────────────────────────────────────
function registrarConexao(data) {
  const d = data || {};
  conexao = { estado: String(d.state || ''), motivo: String(d.statusReason || ''), quando: agoraTexto() };
  console.log(`[CONEXAO] WhatsApp ${conexao.estado}${conexao.motivo ? ' (' + conexao.motivo + ')' : ''}`);
  if (conexao.estado === 'open' && !sync.emAndamento) {
    // ligação nova: zera o placar da sincronização para a tela mostrar só esta
    sync.lotes = 0; sync.recebidas = 0; sync.gravadas = 0; sync.ignoradas = 0; sync.foraDaJanela = 0; sync.erros = 0;
    sync.contatos = new Set(); sync.inicio = ''; sync.fim = ''; sync.progresso = null; sync.ultimoErro = '';
    sync.desde = ''; sync.ate = ''; sync.lidas = 0; sync.naoLidas = 0;
  }
}

function receberLote(lista, extra) {
  const l = Array.isArray(lista) ? lista : (lista ? [lista] : []);
  if (!l.length) return 0;
  fila.push({ lista: l, extra: extra || {} });
  sync.recebidas += l.length;
  sync.lotes += 1;
  if (!sync.inicio) sync.inicio = agoraTexto();
  if (extra && typeof extra.progress !== 'undefined') sync.progresso = extra.progress;
  console.log(`[CONEXAO] histórico: lote de ${l.length} mensagens${extra && extra.progress != null ? ' (' + extra.progress + '%)' : ''}`);
  setImmediate(processarFila);
  return l.length;
}

async function processarFila() {
  if (rodando) return;
  rodando = true;
  sync.emAndamento = true;
  try {
    while (fila.length) {
      const lote = fila.shift();
      const limite = Date.now() / 1000 - JANELA_DIAS * 86400;
      const itens = lote.lista
        .filter(m => m && m.key && m.key.id)
        .map(m => ({ m, ts: Number(m.messageTimestamp) || 0 }))
        .sort((a, b) => a.ts - b.ts);
      for (const it of itens) {
        if (!it.ts || it.ts < limite) { sync.foraDaJanela += 1; continue; }
        try {
          const quando = textoDoTimestamp(it.ts);
          const r = await processar(it.m, 'messages.set', { historico: true, quando });
          if (r && r.gravada) {
            sync.gravadas += 1; if (r.contatoId) sync.contatos.add(r.contatoId);
            if (!sync.desde || quando < sync.desde) sync.desde = quando;
            if (!sync.ate || quando > sync.ate) sync.ate = quando;
          }
          else sync.ignoradas += 1;
        } catch (e) {
          sync.erros += 1; sync.ultimoErro = e.message;
          console.error('[CONEXAO] erro no histórico:', e.message);
        }
      }
      sync.ultimoLote = agoraTexto();
    }
  } finally {
    rodando = false;
    sync.emAndamento = false;
    sync.fim = agoraTexto();
    console.log(`[CONEXAO] histórico processado: ${sync.gravadas} novas, ${sync.ignoradas} já existiam, ${sync.foraDaJanela} fora da janela, ${sync.erros} erros, ${sync.contatos.size} conversas`);
    // o que a recepção já respondeu/viu no celular não fica como não lido (regra do Thiago, 23/11/2026)
    if (sync.gravadas && sync.desde && sync.ate) {
      try { const l = aplicarLidos({ desde: sync.desde, ate: sync.ate }); sync.lidas = l.marcadas; sync.naoLidas = l.mantidas; }
      catch (e) { console.error('[CONEXAO] erro ao marcar lidos:', e.message); }
    }
  }
}

// ─── Lido ou não lido depois da sincronização ──────────────────────────────────────────────────────────────────
// Regra do Thiago (23/11/2026, ao ver 22 conversas "não lidas" no CRM contra bem menos no celular): o que foi
// respondido ou visto no WhatsApp do celular não fica como não lido no CRM. O WhatsApp não manda o estado de leitura
// no histórico, então o CRM deduz: a mensagem do cliente conta como VISTA no celular quando
//   (a) a recepção respondeu (mensagem humana, não Athena/saudação) depois dela; ou
//   (b) depois da resposta só veio cortesia curta ("obrigada", "indo", sem pergunta); ou
//   (c) ela chegou em horário de recepção (seg-sex 8:30-18h, sáb 8:30-13h) pelo menos 30 min antes do fim da
//       sincronização: o celular estava funcionando e a equipe atendeu por ele enquanto o CRM estava fora.
// A (c) conta o TEMPO DE RECEPÇÃO depois da mensagem: 30 min de expediente (seg-sex 8:30-18h, sáb 8:30-13h) entre a
// chegada dela e o fim da janela. Mensagem de domingo à noite conta como vista na segunda às 9h; a de 17:50 só no
// dia seguinte. Com semVista=true a (c) não entra (rotina de 10 min com o CRM no ar: aí o recibo de leitura do
// celular já chega pelo webhook). Equipe/internos e conversas marcadas à mão como não lidas ficam de fora.
// Devolve o que fez; com simular=true só conta.
const CORTESIA_MAX = 30;
// Minutos de recepção entre dois instantes 'AAAA-MM-DD HH:MM:SS' (Brasília). Para ao chegar em 'bastam' minutos.
function minutosRecepcao(aTexto, bTexto, bastam) {
  const p = t => { const [d, h] = String(t).split(' '); const [y, m, dd] = d.split('-').map(Number); const [hh, mm, ss] = (h || '0:0:0').split(':').map(Number); return Date.UTC(y, m - 1, dd, hh || 0, mm || 0, ss || 0); };
  const a = p(aTexto), b = p(bTexto);
  if (!(b > a)) return 0;
  let total = 0;
  const d0 = new Date(a);
  let dia = Date.UTC(d0.getUTCFullYear(), d0.getUTCMonth(), d0.getUTCDate());
  for (let n = 0; n < 400 && dia <= b; n++, dia += 86400000) {
    const sem = new Date(dia).getUTCDay();
    if (sem === 0) continue;
    const ini = dia + (8 * 60 + 30) * 60000, fim = dia + (sem === 6 ? 13 : 18) * 3600000;
    const sobre = Math.min(fim, b) - Math.max(ini, a);
    if (sobre > 0) total += sobre / 60000;
    if (bastam && total >= bastam) break;
  }
  return total;
}
function horarioRecepcao(texto) {
  // texto 'AAAA-MM-DD HH:MM:SS' de Brasília; seg-sex 8:30-18h, sáb 8:30-13h, domingo nunca
  const [, h, m] = String(texto).match(/ (\d{2}):(\d{2})/) || [];
  if (!h) return false;
  const min = Number(h) * 60 + Number(m);
  const dia = new Date(String(texto).slice(0, 10) + 'T12:00:00Z').getUTCDay(); // 0=dom … 6=sáb
  if (dia === 0) return false;
  if (dia === 6) return min >= 8 * 60 + 30 && min < 13 * 60;
  return min >= 8 * 60 + 30 && min < 18 * 60;
}
function aplicarLidos(opcoes) {
  const o = opcoes || {};
  const db = cfg.db;
  const desde = o.desde, ate = o.ate;
  if (!desde || !ate) return { erro: 'faltou janela' };
  const marcas = (cfg.marcas || []).filter(Boolean);
  const automatica = t => marcas.some(m => String(t || '').includes(m));
  const contatos = db.prepare(`SELECT c.id, c.nome, c.telefone, COALESCE(c.last_read_at,'') lr FROM contatos c
    WHERE COALESCE(c.equipe_xr,0) = 0 AND c.telefone NOT LIKE '%@g.us' AND COALESCE(c.manual_unread,0) = 0
      AND EXISTS (SELECT 1 FROM mensagens m WHERE m.contato_id = c.id AND m.direcao = 'in' AND m.created_at >= ? AND m.created_at <= ?)`).all(desde, ate);
  const res = { conversas: contatos.length, marcadas: 0, mantidas: 0, semNaoLida: 0, simular: !!o.simular, detalhes: [] };
  const upd = db.prepare("UPDATE contatos SET last_read_at = MAX(COALESCE(last_read_at,''), ?), manual_unread = 0 WHERE id = ?");
  for (const c of contatos) {
    const msgs = db.prepare('SELECT direcao, texto, tipo, created_at FROM mensagens WHERE contato_id = ? AND created_at <= ? ORDER BY created_at, id').all(c.id, ate);
    const naoLidas = msgs.filter(m => m.direcao === 'in' && m.created_at > c.lr);
    if (!naoLidas.length) { res.semNaoLida++; continue; }
    const respostas = msgs.filter(m => m.direcao === 'out' && m.created_at >= desde && m.created_at <= ate && !automatica(m.texto));
    const ultResp = respostas.length ? respostas[respostas.length - 1].created_at : '';
    const depois = ultResp ? naoLidas.filter(m => m.created_at > ultResp) : naoLidas;
    const cortesia = m => m.tipo === 'text' && String(m.texto || '').trim().length <= CORTESIA_MAX && !/\?/.test(String(m.texto || ''));
    const vistaNoCelular = m => !o.semVista && minutosRecepcao(m.created_at, ate, 30) >= 30;
    const pendentes = depois.filter(m => !cortesia(m) && !vistaNoCelular(m));
    const ultima = msgs[msgs.length - 1].created_at;
    if (!pendentes.length) {
      if (!o.simular) upd.run(ultima, c.id);
      res.marcadas++;
      res.detalhes.push({ contato: c.nome || c.telefone, acao: 'lida', ate: ultima, motivo: !depois.length ? 'respondida' : depois.every(cortesia) ? 'só cortesia depois' : 'vista no celular (30 min de recepção depois dela)' });
    } else {
      res.mantidas++;
      res.detalhes.push({ contato: c.nome || c.telefone, acao: 'continua não lida', pendentes: pendentes.length, ultima: pendentes[pendentes.length - 1].created_at, texto: String(pendentes[pendentes.length - 1].texto || '').slice(0, 60) });
    }
  }
  if (!o.simular && !o.silencioso) console.log(`[CONEXAO] lidos após sincronização (${desde} → ${ate}): ${res.marcadas} conversas marcadas como lidas, ${res.mantidas} continuam não lidas`);
  return res;
}

module.exports = { iniciar, estado, gerarQr, receberLote, registrarConexao, resumoSync, textoDoTimestamp, aplicarLidos, horarioRecepcao, minutosRecepcao, JANELA_DIAS };
