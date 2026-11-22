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
  contatos: new Set(), inicio: '', fim: '', progresso: null, ultimoLote: '', ultimoErro: ''
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
  cfg = { base: opcoes.evoBase, key: opcoes.evoKey, instancia: opcoes.instancia, db: opcoes.db };
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
    progresso: sync.progresso, ultimoLote: sync.ultimoLote, naFila: fila.reduce((s, l) => s + l.lista.length, 0), ultimoErro: sync.ultimoErro
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
          const r = await processar(it.m, 'messages.set', { historico: true, quando: textoDoTimestamp(it.ts) });
          if (r && r.gravada) { sync.gravadas += 1; if (r.contatoId) sync.contatos.add(r.contatoId); }
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
  }
}

module.exports = { iniciar, estado, gerarQr, receberLote, registrarConexao, resumoSync, textoDoTimestamp, JANELA_DIAS };
