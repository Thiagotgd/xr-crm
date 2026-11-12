'use strict';

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const Database = require('better-sqlite3');
const followup = require('./followup');
const alerta = require('./alerta');
const metricas = require('./metricas');
const origem = require('./origem');
const retomada = require('./retomada');
const sombra = require('./sombra');
const leitor = require('./leitor');

const PORT = process.env.PORT || 8095;
const EVO_BASE = process.env.EVO_BASE || 'http://172.18.0.1:8085';
const EVO_KEY = process.env.EVO_KEY || '';
const EVO_INSTANCE = process.env.EVO_INSTANCE || 'xr-whatsapp';
const DB_PATH = path.join(__dirname, 'crm.db');

// ─── Database setup ────────────────────────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS contatos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL DEFAULT '',
    telefone TEXT NOT NULL UNIQUE,
    pet_nome TEXT DEFAULT '',
    pet_especie TEXT DEFAULT '',
    veterinario TEXT DEFAULT '',
    etapa TEXT NOT NULL DEFAULT 'novo',
    notas TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime')),
    updated_at TEXT DEFAULT (datetime('now','localtime')),
    last_message_at TEXT DEFAULT (datetime('now','localtime'))
  );

  CREATE TABLE IF NOT EXISTS mensagens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contato_id INTEGER NOT NULL REFERENCES contatos(id) ON DELETE CASCADE,
    direcao TEXT NOT NULL DEFAULT 'in',
    texto TEXT NOT NULL DEFAULT '',
    tipo TEXT DEFAULT 'text',
    created_at TEXT DEFAULT (datetime('now','localtime'))
  );

  CREATE TABLE IF NOT EXISTS etapas_config (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    ordem INTEGER NOT NULL,
    cor TEXT DEFAULT '#3b82f6'
  );

  CREATE TABLE IF NOT EXISTS etiquetas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    cor TEXT NOT NULL DEFAULT '#3b82f6'
  );

  CREATE TABLE IF NOT EXISTS contato_etiquetas (
    contato_id INTEGER NOT NULL REFERENCES contatos(id) ON DELETE CASCADE,
    etiqueta_id INTEGER NOT NULL REFERENCES etiquetas(id) ON DELETE CASCADE,
    PRIMARY KEY (contato_id, etiqueta_id)
  );

  CREATE INDEX IF NOT EXISTS idx_contatos_etapa ON contatos(etapa);
  CREATE INDEX IF NOT EXISTS idx_contatos_telefone ON contatos(telefone);
  CREATE INDEX IF NOT EXISTS idx_mensagens_contato ON mensagens(contato_id);

  CREATE TABLE IF NOT EXISTS grupos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    jid TEXT UNIQUE NOT NULL,
    nome TEXT DEFAULT '',
    ativo INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now','localtime'))
  );

  CREATE TABLE IF NOT EXISTS grupo_mensagens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    grupo_id INTEGER NOT NULL REFERENCES grupos(id) ON DELETE CASCADE,
    remetente TEXT DEFAULT '',
    remetente_nome TEXT DEFAULT '',
    texto TEXT DEFAULT '',
    tipo TEXT DEFAULT 'text',
    media_path TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime'))
  );

  CREATE INDEX IF NOT EXISTS idx_grupo_msgs ON grupo_mensagens(grupo_id);

  CREATE TABLE IF NOT EXISTS abas_custom (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    icone TEXT DEFAULT '📋',
    cor TEXT DEFAULT '#283593',
    ordem INTEGER DEFAULT 0,
    ativo INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now','localtime'))
  );

  CREATE TABLE IF NOT EXISTS contato_abas (
    contato_id INTEGER NOT NULL REFERENCES contatos(id) ON DELETE CASCADE,
    aba_id INTEGER NOT NULL REFERENCES abas_custom(id) ON DELETE CASCADE,
    PRIMARY KEY (contato_id, aba_id)
  );

  CREATE INDEX IF NOT EXISTS idx_contato_abas ON contato_abas(aba_id);

  CREATE TABLE IF NOT EXISTS etapa_historico (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contato_id INTEGER NOT NULL REFERENCES contatos(id) ON DELETE CASCADE,
    etapa_de TEXT DEFAULT '',
    etapa_para TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now','localtime'))
  );

  CREATE INDEX IF NOT EXISTS idx_etapa_hist_contato ON etapa_historico(contato_id);
  CREATE INDEX IF NOT EXISTS idx_etapa_hist_para ON etapa_historico(etapa_para);

  CREATE TABLE IF NOT EXISTS msgs_rapidas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    titulo TEXT NOT NULL,
    texto TEXT NOT NULL,
    ordem INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now','localtime'))
  );
`);

// Migrations
try { db.exec("ALTER TABLE contatos ADD COLUMN foto_url TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE mensagens ADD COLUMN media_path TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN last_read_at TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN responsavel TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE grupos ADD COLUMN last_read_at TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE grupos ADD COLUMN foto_url TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN equipe_xr INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE contato_abas ADD COLUMN pinado INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE mensagens ADD COLUMN wa_msg_id TEXT DEFAULT ''"); } catch {}
try { db.exec("CREATE INDEX IF NOT EXISTS idx_mensagens_wa_msg_id ON mensagens(wa_msg_id)"); } catch {}
try { db.exec("ALTER TABLE mensagens ADD COLUMN wa_remote_jid TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN manual_unread INTEGER DEFAULT 0"); } catch {}
// 11/11/2026: resposta citando uma mensagem. O trecho citado fica guardado junto da mensagem (de = 'in' cliente, 'out' recepção).
try { db.exec("ALTER TABLE mensagens ADD COLUMN citacao_texto TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE mensagens ADD COLUMN citacao_de TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE mensagens ADD COLUMN editada INTEGER DEFAULT 0"); } catch {}
// 11/11/2026: marcações criadas no CFAZ a partir da conversa (para mostrar "já está na agenda" e permitir desfazer)
db.exec(`CREATE TABLE IF NOT EXISTS agenda_cfaz (
  evento_id INTEGER PRIMARY KEY, contato_id INTEGER NOT NULL, msg_id INTEGER,
  calendario_id INTEGER, calendario_nome TEXT, inicio TEXT, titulo TEXT,
  created_at TEXT DEFAULT (datetime('now','localtime')))`);
try { db.exec("ALTER TABLE mensagens ADD COLUMN reacoes TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE mensagens ADD COLUMN cartao TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE mensagens ADD COLUMN previa TEXT DEFAULT ''"); } catch {}
db.exec("CREATE TABLE IF NOT EXISTS agenda (telefone TEXT PRIMARY KEY, nome TEXT NOT NULL, importado_em TEXT DEFAULT (datetime('now','localtime')))");
try { db.exec("ALTER TABLE contatos ADD COLUMN nome_antes_agenda TEXT DEFAULT ''"); } catch {}
function nomeDaAgenda(telefone) {
  try { const a = db.prepare('SELECT nome FROM agenda WHERE telefone = ?').get(String(telefone || '')); return a ? a.nome : ''; } catch (e) { return ''; }
}
try { db.exec("CREATE INDEX IF NOT EXISTS idx_mensagens_contato_data ON mensagens(contato_id, created_at)"); } catch (e) { console.error('[CRM] índice de mensagens:', e.message); }
try { db.exec("ALTER TABLE contatos ADD COLUMN pinado INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN favorito INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN orcamento_enviado_at TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN ultima_resposta_cliente_at TEXT DEFAULT ''"); } catch {}
followup.initFollowup(db);
origem.init(db);
try { sombra.init(db); } catch (e) { console.error('[SOMBRA] init:', e.message); }
try { leitor.init(db); } catch (e) { console.error('[LEITOR] init:', e.message); }

// Migrate etapas: remove aguardando_exame, confirmado; rename agendamento→agendado; add diversos
try {
  db.exec("UPDATE contatos SET etapa = 'agendado' WHERE etapa = 'agendamento'");
  db.exec("UPDATE contatos SET etapa = 'agendado' WHERE etapa = 'confirmado'");
  db.exec("UPDATE contatos SET etapa = 'novo' WHERE etapa = 'aguardando_exame'");
  db.exec("DELETE FROM etapas_config WHERE id IN ('aguardando_exame', 'confirmado', 'agendamento')");
  db.exec("INSERT OR IGNORE INTO etapas_config (id, label, ordem, cor) VALUES ('agendado', 'Agendado', 3, '#3b82f6')");
  db.exec("INSERT OR IGNORE INTO etapas_config (id, label, ordem, cor) VALUES ('diversos', 'Diversos', 5, '#8b5cf6')");
  db.exec("UPDATE etapas_config SET ordem = 4 WHERE id = 'resultado'");
  db.exec("UPDATE etapas_config SET ordem = 6 WHERE id = 'finalizado'");
} catch {}

// Seed etapa_historico from existing contacts (one-time migration)
try {
  const hasHist = db.prepare("SELECT COUNT(*) as c FROM etapa_historico").get().c;
  if (!hasHist) {
    const existingContatos = db.prepare("SELECT id, etapa, created_at FROM contatos").all();
    const insertHist = db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para, created_at) VALUES (?, '', ?, ?)");
    db.transaction(() => {
      for (const c of existingContatos) insertHist.run(c.id, c.etapa, c.created_at);
    })();
    console.log(`[CRM] Seeded etapa_historico with ${existingContatos.length} entries`);
  }
} catch (e) { console.error('[CRM] Seed hist err:', e.message); }

// Seed default custom tabs + migrate equipe_xr data
try {
  const hasAbas = db.prepare("SELECT COUNT(*) as c FROM abas_custom").get().c;
  if (!hasAbas) {
    db.prepare("INSERT INTO abas_custom (nome, icone, cor, ordem) VALUES ('Equipe XR', '👥', '#283593', 1)").run();
    db.prepare("INSERT INTO abas_custom (nome, icone, cor, ordem) VALUES ('Favoritos', '⭐', '#f59e0b', 2)").run();
    // Migrate existing equipe_xr contacts
    const equipeAba = db.prepare("SELECT id FROM abas_custom WHERE nome = 'Equipe XR'").get();
    if (equipeAba) {
      db.exec("INSERT OR IGNORE INTO contato_abas (contato_id, aba_id) SELECT id, " + equipeAba.id + " FROM contatos WHERE equipe_xr = 1");
    }
  }
} catch (e) { console.error('[CRM] Seed abas err:', e.message); }

// Auth DB (read-only, for team members list)
const AUTH_DB_PATH = path.join(__dirname, '..', 'xr-auth', 'auth.db');
let authDb = null;
try { authDb = new Database(AUTH_DB_PATH, { readonly: true }); } catch (e) { console.log('[CRM] auth.db not available:', e.message); }

const MEDIA_DIR = path.join(__dirname, 'media');
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

// Seed etapas if empty
const etapasCount = db.prepare('SELECT COUNT(*) as c FROM etapas_config').get().c;
if (etapasCount === 0) {
  const insertEtapa = db.prepare('INSERT INTO etapas_config (id, label, ordem, cor) VALUES (?, ?, ?, ?)');
  const etapas = [
    ['novo', 'Novo', 1, '#6366f1'],
    ['orcamento', 'Orçamento', 2, '#f59e0b'],
    ['agendado', 'Agendado', 3, '#3b82f6'],
    ['resultado', 'Resultado', 4, '#f97316'],
    ['diversos', 'Diversos', 5, '#8b5cf6'],
    ['finalizado', 'Finalizado', 6, '#6b7280'],
  ];
  db.transaction(() => { for (const e of etapas) insertEtapa.run(...e); })();
}

// ─── Helpers ───────────────────────────────────────────────────────────────────
function sendJSON(res, status, data) {
  const body = JSON.stringify(data);
  const headers = { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, If-None-Match' };
  // 11/11/2026: a lista de conversas é pedida a cada poucos segundos. Resposta grande de leitura leva uma
  // "impressão digital" (ETag): se a tela já tem essa versão, vai 304 sem corpo; se não, vai compactada.
  const req = res._req;
  if (req && req.method === 'GET' && status === 200 && body.length > 4096) {
    const etag = '"' + crypto.createHash('md5').update(body).digest('hex') + '"';
    headers['ETag'] = etag;
    headers['Cache-Control'] = 'no-store';
    if (req.headers['if-none-match'] === etag) { res.writeHead(304, headers); return res.end(); }
    if (/\bgzip\b/.test(String(req.headers['accept-encoding'] || ''))) {
      try {
        const gz = zlib.gzipSync(body, { level: 4 });
        headers['Content-Encoding'] = 'gzip';
        headers['Vary'] = 'Accept-Encoding';
        res.writeHead(status, headers);
        return res.end(gz);
      } catch (e) { /* cai para o envio simples */ }
    }
  }
  res.writeHead(status, headers);
  res.end(body);
}

// Resumo de uma mensagem para aparecer como trecho citado.
function resumoParaCitar(m) {
  if (!m) return '';
  const t = String(m.texto || '');
  if (m.tipo === 'image') return '📷 Foto' + (t && t !== '[image]' ? ' · ' + t : '');
  if (m.tipo === 'audio') return '🎤 Áudio';
  if (m.tipo === 'document') return '📎 ' + (t && !t.startsWith('[') ? t : 'Documento');
  if (m.tipo === 'video') return '🎥 Vídeo';
  return t;
}

// Mensagem do WhatsApp que responde a outra traz o trecho citado em contextInfo.quotedMessage.
function citacaoDoContexto(msg, data, jid) {
  const vazio = { texto: '', de: '' };
  try {
    const m = msg || {};
    const ctx = (m.extendedTextMessage && m.extendedTextMessage.contextInfo) || (m.imageMessage && m.imageMessage.contextInfo) ||
      (m.documentMessage && m.documentMessage.contextInfo) || (m.audioMessage && m.audioMessage.contextInfo) ||
      (m.videoMessage && m.videoMessage.contextInfo) || (data && data.contextInfo) || null;
    if (!ctx || !ctx.quotedMessage) return vazio;
    const q = ctx.quotedMessage;
    const texto = q.conversation || (q.extendedTextMessage && q.extendedTextMessage.text) ||
      (q.imageMessage && ('📷 Foto' + (q.imageMessage.caption ? ' · ' + q.imageMessage.caption : ''))) ||
      (q.documentMessage && ('📎 ' + (q.documentMessage.fileName || 'Documento'))) ||
      (q.documentWithCaptionMessage && '📎 Documento') ||
      (q.audioMessage && '🎤 Áudio') || (q.videoMessage && '🎥 Vídeo') || (q.stickerMessage && 'Figurinha') || 'Mensagem';
    let de = '';
    if (ctx.stanzaId) {
      const orig = db.prepare('SELECT direcao FROM mensagens WHERE wa_msg_id = ? LIMIT 1').get(String(ctx.stanzaId));
      if (orig) de = orig.direcao;
    }
    if (!de && ctx.participant) {
      const soNum = v => phoneFromJid(String(v || '')).replace(/\D/g, '');
      de = soNum(ctx.participant) && soNum(ctx.participant) === soNum(jid) ? 'in' : 'out';
    }
    return { texto: String(texto).slice(0, 300), de };
  } catch (e) { return vazio; }
}

function parseBody(req, maxSize) {
  const limit = maxSize || 5e6;
  return new Promise((resolve, reject) => {
    let d = '';
    req.on('data', c => { d += c; if (d.length > limit) { req.destroy(); reject(new Error('too large')); } });
    req.on('end', () => { try { resolve(d ? JSON.parse(d) : {}); } catch { resolve({}); } });
    req.on('error', reject);
  });
}

function phoneFromJid(jid) {
  if (!jid) return '';
  return jid.replace('@s.whatsapp.net', '').replace('@c.us', '');
}

function jidFromPhone(phone) {
  const clean = phone.replace(/\D/g, '');
  return clean + '@s.whatsapp.net';
}

// ─── Evolution API send message ────────────────────────────────────────────────
// A Evolution responde 4xx/5xx com um JSON de erro. Sem olhar o código, a mensagem era gravada como enviada.
function erroDaEvolution(res, corpo) {
  if (res.statusCode < 400) return null;
  let motivo = '';
  try { const j = JSON.parse(corpo); motivo = j && j.response && j.response.message ? JSON.stringify(j.response.message) : (j.error || j.message || ''); } catch {}
  if (/"exists":false/.test(String(motivo))) return new Error('este número não tem WhatsApp. Confira o DDD e o número no cadastro do contato.');
  return new Error('WhatsApp recusou o envio (código ' + res.statusCode + ')' + (motivo ? ': ' + String(motivo).slice(0, 160) : ''));
}

function evoSendText(phone, text, quoted) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${EVO_BASE}/message/sendText/${EVO_INSTANCE}`);
    const number = phone.includes('@g.us') ? phone : phone.replace(/\D/g, '');
    const corpo = { number, text };
    if (quoted) corpo.quoted = quoted;
    const payload = JSON.stringify(corpo);
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { const err = erroDaEvolution(res, d); if (err) return reject(err); try { resolve(JSON.parse(d)); } catch { resolve({ raw: d }); } });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('timeout')); });
    req.write(payload);
    req.end();
  });
}

// Chamada genérica à Evolution que devolve o código e o corpo (para rotas que precisam olhar a resposta)
function evoPost(caminho, corpo) {
  return new Promise((resolve, reject) => {
    const url = new URL(EVO_BASE + caminho);
    const payload = JSON.stringify(corpo || {});
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { let j = null; try { j = JSON.parse(d); } catch {} resolve({ status: res.statusCode, json: j, texto: d }); });
    });
    req.on('error', reject);
    req.setTimeout(20000, () => { req.destroy(); reject(new Error('o WhatsApp demorou a responder')); });
    req.write(payload);
    req.end();
  });
}

// ─── Prévia de link ─────────────────────────────────────────────────────────────
const dns = require('dns');
const PREVIA_LIGADA = process.env.PREVIA !== '0';
function ipInterno(ip) {
  const s = String(ip || '').toLowerCase();
  if (s.includes(':')) return s === '::1' || s === '::' || s.startsWith('fc') || s.startsWith('fd') || s.startsWith('fe80') || s.startsWith('::ffff:');
  const p = s.split('.').map(Number);
  if (p.length !== 4 || p.some(n => !(n >= 0 && n <= 255))) return true;
  return p[0] === 0 || p[0] === 10 || p[0] === 127 || (p[0] === 169 && p[1] === 254) || (p[0] === 172 && p[1] >= 16 && p[1] <= 31) ||
    (p[0] === 192 && p[1] === 168) || (p[0] === 100 && p[1] >= 64 && p[1] <= 127) || p[0] >= 224;
}
// O nome só é aceito se apontar para a internet: link mandado por cliente não pode fazer o servidor bater na rede interna.
function resolverSoExterno(host, opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {}; }
  dns.lookup(host, opts, (err, endereco, familia) => {
    if (err) return cb(err);
    const lista = Array.isArray(endereco) ? endereco.map(e => e.address) : [endereco];
    if (!lista.length || lista.some(ipInterno)) return cb(new Error('endereço interno recusado'));
    cb(null, endereco, familia);
  });
}
function baixarDeFora(endereco, limite, saltos) {
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(endereco); } catch (e) { return reject(new Error('endereço inválido')); }
    if (u.protocol !== 'https:' && u.protocol !== 'http:') return reject(new Error('só http e https'));
    if (u.port && u.port !== '80' && u.port !== '443') return reject(new Error('porta recusada'));
    if (u.hostname.includes(':') || u.hostname.startsWith('[') || (/^[\d.]+$/.test(u.hostname) && ipInterno(u.hostname)) || !u.hostname.includes('.')) return reject(new Error('endereço interno recusado'));
    const lib = u.protocol === 'https:' ? https : http;
    const req = lib.get(u, { lookup: resolverSoExterno, timeout: 12000, headers: { 'User-Agent': 'WhatsApp/2.23.20.0', 'Accept': 'text/html,image/*;q=0.9,*/*;q=0.5', 'Accept-Language': 'pt-BR,pt;q=0.9' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        if (saltos <= 0) return reject(new Error('redirecionamentos demais'));
        let prox;
        try { prox = new URL(res.headers.location, u).href; } catch (e) { return reject(new Error('redirecionamento inválido')); }
        return resolve(baixarDeFora(prox, limite, saltos - 1));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('código ' + res.statusCode)); }
      const partes = [];
      let total = 0;
      res.on('data', c => { total += c.length; if (total > limite) { req.destroy(); return reject(new Error('grande demais')); } partes.push(c); });
      res.on('end', () => resolve({ corpo: Buffer.concat(partes), tipo: String(res.headers['content-type'] || ''), final: u.href }));
      res.on('error', reject);
    });
    req.on('timeout', () => req.destroy(new Error('demorou a responder')));
    req.on('error', reject);
  });
}
function desentidade(s) {
  const letra = n => { try { return String.fromCodePoint(n); } catch (e) { return ''; } };
  return String(s).replace(/&#x([0-9a-f]+);/gi, (_, h) => letra(parseInt(h, 16))).replace(/&#(\d+);/g, (_, d) => letra(Number(d)))
    .replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&');
}
function metaDoHtml(html) {
  const m = {};
  for (const tag of html.match(/<meta\b[^>]*>/gi) || []) {
    const chave = (tag.match(/\b(?:property|name)\s*=\s*["']([^"']+)["']/i) || [])[1];
    const valor = (tag.match(/\bcontent\s*=\s*"([^"]*)"/i) || tag.match(/\bcontent\s*=\s*'([^']*)'/i) || [])[1];
    if (chave && valor !== undefined && !(chave.toLowerCase() in m)) m[chave.toLowerCase()] = desentidade(valor).trim();
  }
  const t = (html.match(/<title[^>]*>([^<]*)<\/title>/i) || [])[1];
  if (t) m.title = desentidade(t).trim();
  return m;
}
async function buscarPrevia(endereco) {
  const pag = await baixarDeFora(endereco, 1500000, 4);
  const saida = { titulo: '', descricao: '', site: '', imagem: null };
  if (/^image\/(jpeg|png|webp|gif)/i.test(pag.tipo)) { saida.imagem = pag; return saida; } // o link já é uma imagem
  if (!/html/i.test(pag.tipo)) return saida;
  const m = metaDoHtml(pag.corpo.toString('utf8').slice(0, 700000));
  saida.titulo = String(m['og:title'] || m['twitter:title'] || m.title || '').replace(/^Adobe Acrobat - /, '').slice(0, 140);
  saida.descricao = String(m['og:description'] || m['twitter:description'] || m.description || '').slice(0, 200);
  saida.site = String(m['og:site_name'] || new URL(pag.final).hostname.replace(/^www\./, '')).slice(0, 60);
  let img = m['og:image'] || m['og:image:secure_url'] || m['twitter:image'] || '';
  if (img) {
    try {
      img = new URL(img, pag.final).href;
      // Adobe Acrobat: a imagem é a 1ª página do documento. Pede maior, para dar para ler ao abrir.
      if (/cdn-sharing\.adobecc\.com\/rendition\//.test(img)) img = img.replace(/size=\d+/, 'size=1200');
      const r = await baixarDeFora(img, 4000000, 3);
      if (/^image\/(jpeg|png|webp|gif)/i.test(r.tipo)) saida.imagem = r;
    } catch (e) {}
  }
  return saida;
}
const previaFila = [];
const previaVistas = new Set();
let previaRodando = false;
function agendarPrevia(msgId, texto) {
  if (!PREVIA_LIGADA || previaVistas.has(msgId)) return;
  let url = (String(texto || '').match(/https?:\/\/[^\s<"]+/) || [])[0];
  if (!url) return;
  if (previaVistas.size > 8000) previaVistas.clear();
  previaVistas.add(msgId);
  url = url.replace(/[.,;:!?)\]]+$/, '');
  let host = '';
  try { host = new URL(url).hostname; } catch (e) {}
  // endereços da própria XR e do WhatsApp não têm o que mostrar
  if (!host || /(^|\.)xrdiagnostico\.tech$/i.test(host) || /^(wa\.me|api\.whatsapp\.com|chat\.whatsapp\.com)$/i.test(host)) {
    try { db.prepare("UPDATE mensagens SET previa = '-' WHERE id = ?").run(msgId); } catch (e) {}
    return;
  }
  previaFila.push({ msgId, url });
  rodarPrevias();
}
async function rodarPrevias() {
  if (previaRodando) return;
  previaRodando = true;
  while (previaFila.length) {
    const t = previaFila.shift();
    let gravar = '-'; // '-' = já tentei e não há prévia
    try {
      const p = await buscarPrevia(t.url);
      let arquivo = '';
      if (p.imagem) {
        arquivo = 'previa_' + t.msgId + (/png/i.test(p.imagem.tipo) ? '.png' : /webp/i.test(p.imagem.tipo) ? '.webp' : /gif/i.test(p.imagem.tipo) ? '.gif' : '.jpg');
        fs.writeFileSync(path.join(MEDIA_DIR, arquivo), p.imagem.corpo);
      }
      if (arquivo || p.titulo) gravar = JSON.stringify({ url: t.url, titulo: p.titulo, descricao: p.descricao, site: p.site, imagem: arquivo });
      console.log('[PREVIA] mensagem ' + t.msgId + ': ' + (arquivo ? 'com imagem' : p.titulo ? 'só título' : 'nada'));
    } catch (e) { console.log('[PREVIA] mensagem ' + t.msgId + ': sem prévia (' + e.message + ')'); }
    try { db.prepare('UPDATE mensagens SET previa = ? WHERE id = ?').run(gravar, t.msgId); } catch (e) {}
  }
  previaRodando = false;
}

// ─── Cartão de contato ─────────────────────────────────────────────────────────
function telefoneBonito(t) {
  const d = String(t || '').replace(/\D/g, '');
  if (d.startsWith('55') && d.length === 13) return '+55 ' + d.slice(2, 4) + ' ' + d.slice(4, 9) + '-' + d.slice(9);
  if (d.startsWith('55') && d.length === 12) return '+55 ' + d.slice(2, 4) + ' ' + d.slice(4, 8) + '-' + d.slice(8);
  return '+' + d;
}
// Cartões que vieram numa mensagem do WhatsApp (um contato ou vários) → [{nome, telefone}]
function cartoesDaMensagem(msg) {
  const brutos = msg.contactMessage ? [msg.contactMessage]
    : (msg.contactsArrayMessage && Array.isArray(msg.contactsArrayMessage.contacts)) ? msg.contactsArrayMessage.contacts : [];
  return brutos.map(c => {
    const v = String((c && c.vcard) || '');
    const waid = (v.match(/waid=(\d+)/) || [])[1];
    const tel = waid || String((v.match(/TEL[^:\n]*:([^\n]+)/) || [])[1] || '').replace(/\D/g, '');
    const nome = String((c && c.displayName) || (v.match(/FN:([^\n]+)/) || [])[1] || '').trim().slice(0, 80);
    return { nome, telefone: tel };
  }).filter(c => c.nome || c.telefone).slice(0, 20);
}
// Envia cartões para um telefone ou grupo. Devolve a resposta da Evolution (com a chave da mensagem).
async function enviarCartao(alvo, lista) {
  const numero = String(alvo).includes('@g.us') ? String(alvo) : String(alvo).replace(/\D/g, '');
  const r = await evoPost('/message/sendContact/' + EVO_INSTANCE, {
    number: numero,
    contact: lista.map(c => ({ fullName: c.nome || telefoneBonito(c.telefone), wuid: String(c.telefone), phoneNumber: telefoneBonito(c.telefone) }))
  });
  if (r.status >= 400) {
    const m = r.json && r.json.response && r.json.response.message ? JSON.stringify(r.json.response.message) : (r.json && (r.json.error || r.json.message)) || ('código ' + r.status);
    if (/"exists":false/.test(String(m))) throw new Error('este número não tem WhatsApp. Confira o DDD e o número no cadastro do contato.');
    throw new Error('o WhatsApp não aceitou o contato: ' + String(m).slice(0, 160));
  }
  return r.json || {};
}

// ─── Reações (👍 ❤️ …) ─────────────────────────────────────────────────────────
// Ficam na própria mensagem reagida, em JSON: {"in":"👍","out":"❤️"} (in = cliente, out = recepção). Emoji vazio tira.
function gravarReacao(waMsgId, quem, emoji) {
  const m = db.prepare('SELECT id, reacoes FROM mensagens WHERE wa_msg_id = ? LIMIT 1').get(String(waMsgId || ''));
  if (!m) return false;
  let r = {};
  try { r = JSON.parse(m.reacoes || '{}') || {}; } catch {}
  if (emoji) r[quem] = String(emoji).slice(0, 16); else delete r[quem];
  db.prepare('UPDATE mensagens SET reacoes = ? WHERE id = ?').run(Object.keys(r).length ? JSON.stringify(r) : '', m.id);
  return true;
}

// Endereço da conversa como a Evolution guardou (pode ser o endereço novo do WhatsApp, em LID)
async function jidDaMensagem(msg) {
  let jid = msg.wa_remote_jid || '';
  try {
    const achada = await evoPost('/chat/findMessages/' + EVO_INSTANCE, { where: { key: { id: msg.wa_msg_id } } });
    const rec = achada.json && achada.json.messages && achada.json.messages.records && achada.json.messages.records[0];
    if (rec && rec.key && rec.key.remoteJid) jid = rec.key.remoteJid;
  } catch {}
  if (!jid) { const c = db.prepare('SELECT telefone FROM contatos WHERE id = ?').get(msg.contato_id); jid = jidFromPhone(c ? c.telefone : ''); }
  return jid;
}

// ─── Número digitado → número como o WhatsApp conhece ───────────────────────────
// A recepção digita "(37) 99820-5137"; o WhatsApp só acha com o 55 na frente e, em muitos DDDs, sem o nono dígito.
// Pergunta à Evolution e devolve o número certo. Número de fora do Brasil (já com o código do país) também passa.
async function numeroNoWhatsApp(digitado) {
  const d = String(digitado || '').replace(/\D/g, '').replace(/^0+/, '');
  const tentativas = (d.length === 10 || d.length === 11) ? ['55' + d, d] : [d];
  try {
    const r = await evoPost('/chat/whatsappNumbers/' + EVO_INSTANCE, { numbers: tentativas });
    if (r.status < 400 && Array.isArray(r.json)) {
      for (const t of tentativas) {
        const x = r.json.find(y => y && y.exists && String(y.number) === t);
        if (x) return { telefone: /@s\.whatsapp\.net$/.test(String(x.jid || '')) ? String(x.jid).split('@')[0] : t, existe: true, conferido: true };
      }
      return { telefone: tentativas[0], existe: false, conferido: true };
    }
  } catch (e) { console.error('[CRM] não consegui conferir o número no WhatsApp:', e.message); }
  return { telefone: tentativas[0], existe: true, conferido: false };
}

// ─── Quem o WhatsApp da recepção conhece ───────────────────────────────────
// A Evolution guarda os contatos do aparelho com o nome que a PESSOA pôs no perfil dela (o apelido salvo na agenda
// do celular não chega aqui). Serve para a busca achar gente que ainda não tem conversa no CRM. Relido a cada 10 min.
function semAcento(s) { return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase(); }
let contatosWhats = { em: 0, lista: [] };
async function carregarContatosWhats() {
  if (Date.now() - contatosWhats.em < 10 * 60000) return contatosWhats.lista;
  try {
    const r = await evoPost('/chat/findContacts/' + EVO_INSTANCE, { where: {} });
    if (r.status < 400 && Array.isArray(r.json)) {
      contatosWhats = { em: Date.now(), lista: r.json
        .filter(c => c && c.type === 'contact' && /@s\.whatsapp\.net$/.test(String(c.remoteJid || '')) && c.pushName && !/^[+\d\s.()-]*$/.test(String(c.pushName)))
        .map(c => ({ telefone: String(c.remoteJid).split('@')[0], nome: String(c.pushName).trim(), chave: semAcento(c.pushName), foto: c.profilePicUrl || '' })) };
      return contatosWhats.lista;
    }
  } catch (e) { console.error('[CRM] contatos do WhatsApp:', e.message); }
  contatosWhats.em = Date.now() - 9 * 60000; // falhou: usa o que tinha e tenta de novo em 1 minuto
  return contatosWhats.lista;
}

// Mark messages as read on WhatsApp (sync CRM → WhatsApp)
function evoMarkAsRead(phone, ids) {
  const jid = phone.includes('@') ? phone : `${phone}@s.whatsapp.net`;
  const lista = (ids && ids.length ? ids : ['all']).map(id => ({ remoteJid: jid, fromMe: false, id }));
  console.log(`[CRM] lido → WhatsApp: ${phone} (${ids && ids.length ? ids.length + ' mensagens' : 'sem identificador'})`);
  const payload = JSON.stringify({ readMessages: lista });
  const url = new URL(`${EVO_BASE}/chat/markMessageAsRead/${EVO_INSTANCE}`);
  const req = http.request({
    hostname: url.hostname, port: url.port, path: url.pathname,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
  }, res => { let d = ''; res.on('data', c => d += c); res.on('end', () => {}); });
  req.on('error', () => {});
  req.setTimeout(5000, () => req.destroy());
  req.write(payload);
  req.end();
}

function evoDownloadMedia(messageKey) {
  return new Promise((resolve) => {
    const url = new URL(`${EVO_BASE}/chat/getBase64FromMediaMessage/${EVO_INSTANCE}`);
    const payload = JSON.stringify({ message: { key: messageKey }, convertToMp4: false });
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => {
        try { const j = JSON.parse(d); resolve(j); } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(30000, () => { req.destroy(); resolve(null); });
    req.write(payload);
    req.end();
  });
}

function evoSendMedia(phone, mediatype, base64, caption, fileName, mimetype) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${EVO_BASE}/message/sendMedia/${EVO_INSTANCE}`);
    const corpo = { number: phone.includes('@g.us') ? phone : phone.replace(/\D/g, ''), mediatype, media: base64, caption: caption || '', fileName: fileName || '' };
    if (mimetype) corpo.mimetype = mimetype;
    const payload = JSON.stringify(corpo);
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { const err = erroDaEvolution(res, d); if (err) return reject(err); try { resolve(JSON.parse(d)); } catch { resolve({ raw: d }); } });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout')); });
    req.write(payload);
    req.end();
  });
}

function evoFetchProfilePic(phone) {
  return new Promise((resolve) => {
    const url = new URL(`${EVO_BASE}/chat/fetchProfilePictureUrl/${EVO_INSTANCE}`);
    const number = phone.includes('@') ? phone : phone.replace(/\D/g, '');
    const payload = JSON.stringify({ number });
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => {
        try { const j = JSON.parse(d); resolve(j.profilePictureUrl || ''); } catch { resolve(''); }
      });
    });
    req.on('error', () => resolve(''));
    req.setTimeout(10000, () => { req.destroy(); resolve(''); });
    req.write(payload);
    req.end();
  });
}

function evoDeleteMessage(remoteJid, msgId, fromMe) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${EVO_BASE}/chat/deleteMessageForEveryone/${EVO_INSTANCE}`);
    const payload = JSON.stringify({ id: msgId, fromMe, remoteJid });
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname,
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve({ raw: d }); } });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('timeout')); });
    req.write(payload);
    req.end();
  });
}

let frasesCache = {}; // frases frequentes da recepção (refeito a cada 15 min)

// Texto curto de uma mensagem para o card do Follow-up (áudio vira a transcrição; foto e PDF viram o que o leitor identificou).
function textoParaCard(m) {
  const l = m.leitura;
  if (l && l.tipo === 'audio') return '🎤 ' + String(l.texto || '').slice(0, 500);
  if (l && l.tipo === 'pedido_exame') return '📄 Pedido de exame' + (l.exames && l.exames.length ? ': ' + l.exames.join('; ') : '');
  if (l) return (l.icone || '📎') + ' ' + (l.rotulo || 'Anexo');
  const t = String(m.texto || '');
  if (m.tipo === 'audio') return '🎤 Áudio';
  if (m.tipo === 'image') return '📷 Foto' + (t && t !== '[image]' ? ' · ' + t.slice(0, 300) : '');
  if (m.tipo === 'document') return '📎 ' + (t && !t.startsWith('[') ? t.split('\n')[0].slice(0, 120) : 'Documento');
  return t.slice(0, 500);
}

// ─── CFAZ: agenda ───────────────────────────────────────────────────────────────────────────────
// Mesma credencial que a fila usa; fica em secrets/cfaz.key (fora do git) ou na variável CFAZ_API_TOKEN.
const CFAZ_BASE = process.env.CFAZ_BASE || 'https://max.cfaz.net';
let CFAZ_TOKEN = process.env.CFAZ_API_TOKEN || '';
try { if (!CFAZ_TOKEN) CFAZ_TOKEN = fs.readFileSync(path.join(__dirname, 'secrets', 'cfaz.key'), 'utf8').trim(); } catch {}
function cfaz(method, caminho, corpo) {
  return new Promise((resolve, reject) => {
    if (!CFAZ_TOKEN) return reject(new Error('credencial do CFAZ não configurada no CRM'));
    const u = new URL(CFAZ_BASE + caminho);
    const lib = u.protocol === 'https:' ? https : http;
    const payload = corpo ? JSON.stringify(corpo) : null;
    const headers = { 'Authorization': 'Token token=' + CFAZ_TOKEN, 'Accept': 'application/json' };
    if (payload) { headers['Content-Type'] = 'application/json'; headers['Content-Length'] = Buffer.byteLength(payload); }
    const req = lib.request({ hostname: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80), path: u.pathname + u.search, method, headers }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { let j = null; try { j = d ? JSON.parse(d) : null; } catch {} resolve({ status: res.statusCode, json: j, texto: d }); });
    });
    req.on('error', e => reject(new Error('o CFAZ não respondeu: ' + e.message)));
    req.setTimeout(25000, () => { req.destroy(); reject(new Error('o CFAZ demorou mais de 25 s')); });
    if (payload) req.write(payload);
    req.end();
  });
}
let cfazCalCache = { em: 0, lista: [] };
async function cfazCalendarios() {
  if (cfazCalCache.lista.length && Date.now() - cfazCalCache.em < 15 * 60 * 1000) return cfazCalCache.lista;
  const r = await cfaz('GET', '/api/v1/calendars.json');
  const L = Array.isArray(r.json) ? r.json : (r.json && r.json.data) || [];
  if (r.status !== 200 || !L.length) throw new Error('o CFAZ não devolveu as agendas (código ' + r.status + ')');
  cfazCalCache = { em: Date.now(), lista: L.filter(c => c.enabled).map(c => ({ id: c.id, nome: String(c.name || '').trim(), duracao: Number(c.default_event_duration) || 20 })) };
  return cfazCalCache.lista;
}
let cfazEvCache = { em: 0, lista: [] };
async function cfazEventos(forcar) {
  if (!forcar && cfazEvCache.em && Date.now() - cfazEvCache.em < 45 * 1000) return cfazEvCache.lista;
  const todos = [], vistos = new Set();
  for (let p = 1; p <= 8; p++) {
    const r = await cfaz('GET', '/api/v1/events.json?per_page=100&page=' + p);
    const L = Array.isArray(r.json) ? r.json : (r.json && r.json.data);
    if (r.status !== 200 || !Array.isArray(L)) throw new Error('o CFAZ não devolveu as marcações (código ' + r.status + ')');
    const novos = L.filter(e => !vistos.has(e.id));
    if (!novos.length) break;
    for (const e of novos) { vistos.add(e.id); todos.push(e); }
  }
  cfazEvCache = { em: Date.now(), lista: todos };
  return todos;
}
function telefoneCfaz(tel) {
  const d = String(tel || '').replace(/\D/g, '');
  if (!d) return '';
  return d.length >= 12 && d.startsWith('55') ? '+55 ' + d.slice(2) : '+55 ' + d;
}
// Junta a cada mensagem a marcação do CFAZ criada a partir dela (se houver).
function anexarAgenda(mensagens) {
  if (!mensagens || !mensagens.length) return;
  const ids = mensagens.map(m => m.id);
  const rows = db.prepare('SELECT * FROM agenda_cfaz WHERE msg_id IN (' + ids.map(() => '?').join(',') + ')').all(...ids);
  // 12/11/2026: uma mensagem pode ter várias marcações (ex.: ressonância + ecocardiograma) → lista, na ordem do horário
  const por = {};
  for (const r of rows) (por[r.msg_id] = por[r.msg_id] || []).push({ evento_id: r.evento_id, calendario_id: r.calendario_id, calendario: r.calendario_nome, inicio: r.inicio, titulo: r.titulo });
  for (const m of mensagens) if (por[m.id]) m.cfaz = por[m.id].sort((a, b) => String(a.inicio).localeCompare(String(b.inicio)));
}

// ─── Routes ────────────────────────────────────────────────────────────────────
async function handleRequest(req, res) {
  const urlObj = new URL(req.url, 'http://localhost');
  const pathname = urlObj.pathname;
  const method = req.method;
  res._req = req;

  if (method === 'OPTIONS') return sendJSON(res, 200, {});

  // Serve HTML
  if (method === 'GET' && (pathname === '/' || pathname === '/crm' || pathname === '/crm/')) {
    const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf-8');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(html);
  }

  // ─── Serve static assets ──────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/logo-xr.png') {
    const logoPath = path.join(__dirname, 'logo-xr.png');
    if (fs.existsSync(logoPath)) {
      res.writeHead(200, { 'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=604800' });
      return fs.createReadStream(logoPath).pipe(res);
    }
  }

  if (method === 'GET' && pathname === '/chat-bg.jpg') {
    const bgPath = path.join(__dirname, 'chat-bg.jpg');
    if (fs.existsSync(bgPath)) {
      res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'public, max-age=604800' });
      return fs.createReadStream(bgPath).pipe(res);
    }
  }

  // ─── Serve media files ────────────────────────────────────────────────────
  if (method === 'GET' && pathname.startsWith('/arquivos/')) {
    const nome = path.basename(pathname);
    const arq = path.join(__dirname, 'arquivos', nome);
    if (!/^[\w.-]+$/.test(nome) || !fs.existsSync(arq)) { res.writeHead(404); return res.end('Not found'); }
    const tipos = { '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.pdf': 'application/pdf', '.js': 'application/javascript; charset=utf-8' };
    res.writeHead(200, { 'Content-Type': tipos[path.extname(nome).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'public, max-age=3600' });
    return fs.createReadStream(arq).pipe(res);
  }

  if (method === 'GET' && pathname.startsWith('/media/')) {
    const filename = path.basename(pathname);
    const filePath = path.join(MEDIA_DIR, filename);
    if (!fs.existsSync(filePath)) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(filename).toLowerCase();
    const mimeMap = { '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp', '.mp4': 'video/mp4', '.ogg': 'audio/ogg', '.mp3': 'audio/mpeg', '.pdf': 'application/pdf' };
    res.writeHead(200, { 'Content-Type': mimeMap[ext] || 'application/octet-stream', 'Cache-Control': 'public, max-age=86400' });
    return fs.createReadStream(filePath).pipe(res);
  }

  // ─── API: Etapas ──────────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/etapas') {
    const rows = db.prepare('SELECT * FROM etapas_config ORDER BY ordem').all();
    return sendJSON(res, 200, rows);
  }

  // ─── API: Etiquetas CRUD ────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/etiquetas') {
    return sendJSON(res, 200, db.prepare('SELECT * FROM etiquetas ORDER BY nome').all());
  }
  if (method === 'POST' && pathname === '/api/etiqueta') {
    const body = await parseBody(req);
    if (!body.nome) return sendJSON(res, 400, { error: 'nome required' });
    const r = db.prepare('INSERT INTO etiquetas (nome, cor) VALUES (?, ?)').run(body.nome.trim(), body.cor || '#3b82f6');
    return sendJSON(res, 201, { ok: true, id: r.lastInsertRowid });
  }
  if (method === 'PUT' && pathname.match(/^\/api\/etiqueta\/\d+$/)) {
    const id = pathname.split('/').pop();
    const body = await parseBody(req);
    const fields = [];
    const values = [];
    if (body.nome !== undefined) { fields.push('nome = ?'); values.push(body.nome.trim()); }
    if (body.cor !== undefined) { fields.push('cor = ?'); values.push(body.cor); }
    if (fields.length === 0) return sendJSON(res, 400, { error: 'No fields' });
    values.push(id);
    db.prepare(`UPDATE etiquetas SET ${fields.join(', ')} WHERE id = ?`).run(...values);
    return sendJSON(res, 200, { ok: true });
  }
  if (method === 'DELETE' && pathname.match(/^\/api\/etiqueta\/\d+$/)) {
    const id = pathname.split('/').pop();
    db.prepare('DELETE FROM etiquetas WHERE id = ?').run(id);
    return sendJSON(res, 200, { ok: true });
  }
  // Add/remove etiqueta from contato
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/etiqueta$/)) {
    const contatoId = pathname.split('/')[3];
    const body = await parseBody(req);
    if (!body.etiqueta_id) return sendJSON(res, 400, { error: 'etiqueta_id required' });
    try {
      db.prepare('INSERT OR IGNORE INTO contato_etiquetas (contato_id, etiqueta_id) VALUES (?, ?)').run(contatoId, body.etiqueta_id);
    } catch(e) {}
    return sendJSON(res, 200, { ok: true });
  }
  if (method === 'DELETE' && pathname.match(/^\/api\/contato\/\d+\/etiqueta\/\d+$/)) {
    const parts = pathname.split('/');
    const contatoId = parts[3];
    const etiquetaId = parts[5];
    db.prepare('DELETE FROM contato_etiquetas WHERE contato_id = ? AND etiqueta_id = ?').run(contatoId, etiquetaId);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Contatos (board view) ───────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/contatos') {
    const rows = db.prepare(`
      SELECT c.*,
        (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id) as total_msgs,
        (SELECT texto FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg,
        (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id AND direcao = 'in' AND created_at > COALESCE(NULLIF(c.last_read_at,''), '1970-01-01')) as nao_lidas,
        (SELECT direcao FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_dir,
        (SELECT created_at FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_at
      FROM contatos c
      WHERE NOT (c.etapa = 'finalizado' AND c.updated_at < datetime('now','localtime','-2 days'))
        AND c.id NOT IN (SELECT contato_id FROM contato_abas)
      ORDER BY c.last_message_at DESC
    `).all();
    // Attach etiquetas to each contato
    const getEtiquetas = db.prepare('SELECT e.* FROM etiquetas e JOIN contato_etiquetas ce ON ce.etiqueta_id = e.id WHERE ce.contato_id = ?');
    for (const row of rows) { row.etiquetas = getEtiquetas.all(row.id); }
    return sendJSON(res, 200, rows);
  }

  // ─── API: Single contato with messages ────────────────────────────────────
  if (method === 'GET' && pathname.match(/^\/api\/contato\/\d+\/mensagens$/)) {
    const id = pathname.split('/')[3];
    const antes = Number(urlObj.searchParams.get('antes') || 0);
    const ref = antes ? db.prepare('SELECT id, created_at FROM mensagens WHERE id = ? AND contato_id = ?').get(antes, id) : null;
    if (!ref) return sendJSON(res, 400, { error: 'informe a mensagem de referência (antes)' });
    const msgs = db.prepare('SELECT * FROM mensagens WHERE contato_id = ? AND (created_at < ? OR (created_at = ? AND id < ?)) ORDER BY created_at DESC, id DESC LIMIT 51').all(id, ref.created_at, ref.created_at, ref.id);
    const temMais = msgs.length > 50;
    if (temMais) msgs.pop();
    try { leitor.anexarLeituras(db, msgs); } catch (e) { console.error('[LEITOR] anexar:', e.message); }
    try { anexarAgenda(msgs); } catch (e) { console.error('[CFAZ] anexar:', e.message); }
    return sendJSON(res, 200, { mensagens: msgs, tem_mais: temMais });
  }

  if (method === 'GET' && pathname.match(/^\/api\/contato\/\d+$/)) {
    const id = pathname.split('/').pop();
    const contato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Not found' });
    contato.mensagens = db.prepare('SELECT * FROM mensagens WHERE contato_id = ? ORDER BY created_at DESC, id DESC LIMIT 51').all(id);
    contato.tem_mais = contato.mensagens.length > 50;
    if (contato.tem_mais) contato.mensagens.pop();
    // links ainda sem prévia (mensagens de antes de 12/11/2026): busca em segundo plano; aparece na atualização seguinte
    try {
      let n = 0;
      for (const m of contato.mensagens) {
        if (n >= 6) break;
        if (m.tipo === 'text' && !m.previa && /https?:\/\//.test(m.texto || '')) { agendarPrevia(m.id, m.texto); n++; }
      }
    } catch (e) {}
    // Foto ou PDF do cliente já lido pela IA (pedido do veterinário etc.): a leitura vai junto da mensagem.
    try { leitor.anexarLeituras(db, contato.mensagens); } catch (e) { console.error('[LEITOR] anexar:', e.message); }
    try { anexarAgenda(contato.mensagens); } catch (e) { console.error('[CFAZ] anexar:', e.message); }
    contato.etiquetas = db.prepare('SELECT e.* FROM etiquetas e JOIN contato_etiquetas ce ON ce.etiqueta_id = e.id WHERE ce.contato_id = ?').all(id);
    // 11/11/2026: a tela atualiza a conversa aberta a cada 5 s. Quando a aba do CRM está em segundo plano ela
    // manda ?visto=0, e aí NÃO se marca como lido: antes, uma conversa esquecida aberta apagava o aviso verde
    // de mensagem nova no WhatsApp sem ninguém ter lido.
    const visto = urlObj.searchParams.get('visto') !== '0';
    if (visto) {
      // Mark as read + reset manual_unread
      db.prepare("UPDATE contatos SET last_read_at = datetime('now','localtime'), manual_unread = 0 WHERE id = ?").run(id);
      // Sync: mark as read on real WhatsApp too — só se havia algo por ler (antes ia a cada atualização)
      // 12/11/2026: o aviso precisa dizer QUAIS mensagens foram lidas (o identificador de cada uma). Antes ia
      // id "all", que o WhatsApp ignora: a conversa aberta no CRM continuava como não lida no WhatsApp da recepção.
      const naoLidas = db.prepare("SELECT wa_msg_id FROM mensagens WHERE contato_id = ? AND direcao = 'in' AND wa_msg_id != '' AND created_at > COALESCE(NULLIF(?, ''), '1970-01-01') ORDER BY id DESC LIMIT 40").all(id, contato.last_read_at || '').map(r => r.wa_msg_id);
      if (!naoLidas.length && contato.manual_unread) {
        const ult = db.prepare("SELECT wa_msg_id FROM mensagens WHERE contato_id = ? AND direcao = 'in' AND wa_msg_id != '' ORDER BY id DESC LIMIT 1").get(id);
        if (ult) naoLidas.push(ult.wa_msg_id);
      }
      if (contato.telefone && naoLidas.length) evoMarkAsRead(contato.telefone, naoLidas);
    }
    return sendJSON(res, 200, contato);
  }

  // ─── API: Create contato ──────────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/contato') {
    const body = await parseBody(req);
    const { nome, telefone, pet_nome, pet_especie, veterinario, etapa, notas } = body;
    if (!telefone) return sendJSON(res, 400, { error: 'telefone required' });
    // confere o número no WhatsApp: põe o 55, acerta o nono dígito e avisa se o número não tem WhatsApp
    const num = await numeroNoWhatsApp(telefone);
    if (num.telefone.length < 8) return sendJSON(res, 400, { error: 'Telefone incompleto. Digite com o DDD.' });
    if (num.conferido && !num.existe) return sendJSON(res, 400, { error: 'Este número não tem WhatsApp. Confira o DDD e o número (não precisa digitar o 55).' });
    const phone = num.telefone;
    const jaTem = db.prepare('SELECT id, nome FROM contatos WHERE telefone = ?').get(phone);
    if (jaTem) {
      // já existe (ex.: a pessoa já tinha escrito e o contato estava sem nome): completa o nome e devolve o mesmo contato
      if (nome && !jaTem.nome) db.prepare('UPDATE contatos SET nome = ? WHERE id = ?').run(nome, jaTem.id);
      return sendJSON(res, 200, { ok: true, id: jaTem.id, existente: true });
    }
    try {
      const finalEtapa = etapa || 'novo';
      const result = db.prepare(`
        INSERT INTO contatos (nome, telefone, pet_nome, pet_especie, veterinario, etapa, notas)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(nome || '', phone, pet_nome || '', pet_especie || '', veterinario || '', finalEtapa, notas || '');
      db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, '', ?)").run(result.lastInsertRowid, finalEtapa);
      return sendJSON(res, 201, { ok: true, id: result.lastInsertRowid });
    } catch (e) {
      if (e.message.includes('UNIQUE')) return sendJSON(res, 409, { error: 'Contato com este telefone já existe' });
      throw e;
    }
  }

  // ─── API: Update contato ──────────────────────────────────────────────────
  if (method === 'PUT' && pathname.match(/^\/api\/contato\/\d+$/)) {
    const id = pathname.split('/').pop();
    const body = await parseBody(req);
    const fields = [];
    const values = [];
    for (const key of ['nome', 'telefone', 'pet_nome', 'pet_especie', 'veterinario', 'etapa', 'notas', 'responsavel']) {
      if (body[key] !== undefined) {
        fields.push(`${key} = ?`);
        values.push(key === 'telefone' ? body[key].replace(/\D/g, '') : body[key]);
      }
    }
    if (fields.length === 0) return sendJSON(res, 400, { error: 'No fields to update' });
    // Log etapa change if applicable
    if (body.etapa !== undefined) {
      const old = db.prepare('SELECT etapa FROM contatos WHERE id = ?').get(id);
      if (old && old.etapa !== body.etapa) {
        db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, ?)").run(id, old.etapa, body.etapa);
      }
    }
    fields.push("updated_at = datetime('now','localtime')");
    values.push(id);
    db.prepare(`UPDATE contatos SET ${fields.join(', ')} WHERE id = ?`).run(...values);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Move contato to etapa ───────────────────────────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/mover$/)) {
    const id = pathname.split('/')[3];
    const body = await parseBody(req);
    if (!body.etapa) return sendJSON(res, 400, { error: 'etapa required' });
    const old = db.prepare('SELECT etapa FROM contatos WHERE id = ?').get(id);
    db.prepare("UPDATE contatos SET etapa = ?, updated_at = datetime('now','localtime') WHERE id = ?").run(body.etapa, id);
    if (old && old.etapa !== body.etapa) db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, ?)").run(id, old.etapa, body.etapa);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Marcar como não lido ─────────────────────────────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/marcar-nao-lido$/)) {
    const id = pathname.split('/')[3];
    db.prepare('UPDATE contatos SET manual_unread = 1 WHERE id = ?').run(id);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Toggle pin contato ──────────────────────────────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/pin$/)) {
    const id = pathname.split('/')[3];
    const current = db.prepare('SELECT pinado FROM contatos WHERE id = ?').get(id);
    if (!current) return sendJSON(res, 404, { error: 'Not found' });
    const newVal = current.pinado ? 0 : 1;
    db.prepare('UPDATE contatos SET pinado = ? WHERE id = ?').run(newVal, id);
    return sendJSON(res, 200, { ok: true, pinado: newVal });
  }

  // POST /api/contato/:id/favorito (toggle)
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/favorito$/)) {
    const id = pathname.split('/')[3];
    const current = db.prepare('SELECT favorito FROM contatos WHERE id = ?').get(id);
    if (!current) return sendJSON(res, 404, { error: 'Not found' });
    const newVal = current.favorito ? 0 : 1;
    db.prepare('UPDATE contatos SET favorito = ? WHERE id = ?').run(newVal, id);
    return sendJSON(res, 200, { ok: true, favorito: newVal });
  }

  // POST /api/contato/:id/orcamento-enviado — mark quote as sent + auto-move
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/orcamento-enviado$/)) {
    const id = pathname.split('/')[3];
    const contato = db.prepare('SELECT etapa FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Not found' });
    const etapasAntes = ['novo', 'agendado'];
    if (etapasAntes.includes(contato.etapa)) {
      db.prepare("UPDATE contatos SET etapa = 'orcamento', orcamento_enviado_at = datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE id = ?").run(id);
      db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'orcamento')").run(id, contato.etapa);
    } else {
      db.prepare("UPDATE contatos SET orcamento_enviado_at = datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE id = ?").run(id);
    }
    return sendJSON(res, 200, { ok: true });
  }

  // POST /api/contato/:id/agendar — mark as scheduled + auto-move
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/agendar$/)) {
    const id = pathname.split('/')[3];
    const contato = db.prepare('SELECT etapa FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Not found' });
    const etapasAntes = ['novo', 'orcamento'];
    if (etapasAntes.includes(contato.etapa)) {
      db.prepare("UPDATE contatos SET etapa = 'agendado', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
      db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'agendado')").run(id, contato.etapa);
    }
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Contatos (conversas view - all contacts) ────────────────────────
  if (method === 'GET' && pathname === '/api/contatos/conversas') {
    // 11/11/2026: a tela pede esta lista a cada poucos segundos. Antes vinham os ~2.500 contatos com todos os
    // campos (2,2 MB). Agora: só os campos que a lista usa e, por padrão, as conversas dos últimos 45 dias mais
    // as fixadas, favoritas, marcadas como não lidas ou com etiqueta. ?q= busca em TODOS os contatos; ?tudo=1 traz tudo.
    const q = (urlObj.searchParams.get('q') || '').trim();
    const tudo = urlObj.searchParams.get('tudo') === '1';
    const campos = `SELECT c.id, c.nome, c.telefone, c.pet_nome, c.etapa, c.foto_url, c.pinado, c.favorito, c.manual_unread, c.equipe_xr,
        c.orcamento_enviado_at, c.ultima_resposta_cliente_at, c.last_message_at, c.created_at, c.origem,
        substr(m.texto, 1, 100) as ultima_msg, m.direcao as ultima_msg_dir, m.created_at as ultima_msg_at, m.tipo as ultima_msg_tipo,
        (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id AND direcao = 'in' AND created_at > COALESCE(NULLIF(c.last_read_at,''), '1970-01-01')) as nao_lidas
      FROM contatos c
      LEFT JOIN mensagens m ON m.id = (SELECT id FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC, id DESC LIMIT 1)`;
    let rows;
    if (q.length >= 2) {
      const like = '%' + q + '%';
      const digitos = q.replace(/[^0-9]/g, '');
      const likeTel = digitos.length >= 3 ? '%' + digitos + '%' : like;
      // (nome_antes_agenda = nome que o contato tinha antes de receber o apelido da agenda: continua achando por ele)
      rows = db.prepare(campos + ` WHERE c.nome LIKE ? OR c.telefone LIKE ? OR c.pet_nome LIKE ? OR c.nome_antes_agenda LIKE ? ORDER BY c.last_message_at DESC LIMIT 80`).all(like, likeTel, like, like);
    } else {
      const recentes = tudo ? '' : ` AND (c.last_message_at >= datetime('now','localtime','-45 days') OR c.pinado = 1 OR c.favorito = 1 OR c.manual_unread = 1 OR EXISTS (SELECT 1 FROM contato_etiquetas ce WHERE ce.contato_id = c.id))`;
      rows = db.prepare(campos + ` WHERE NOT (c.etapa = 'finalizado' AND c.updated_at < datetime('now','localtime','-2 days'))` + recentes + ` ORDER BY c.pinado DESC, c.last_message_at DESC`).all();
    }
    // Busca: junta quem o WhatsApp da recepção conhece pelo nome do perfil — gente que ainda não tem conversa no
    // CRM (vem marcada so_whatsapp, a tela cria o contato ao clicar) e contatos do CRM gravados com outro nome.
    const doWhats = [];
    if (q.length >= 3) {
      const alvo = semAcento(q);
      const dig = q.replace(/[^0-9]/g, '');
      const jaVeio = new Set(rows.map(r => r.telefone));
      // 1º a agenda importada do celular da recepção (o nome que a recepção deu ao contato)
      for (const a of db.prepare('SELECT telefone, nome FROM agenda').all()) {
        if (doWhats.length >= 15) break;
        if (jaVeio.has(a.telefone)) continue;
        if (!(semAcento(a.nome).includes(alvo) || (dig.length >= 5 && a.telefone.includes(dig)))) continue;
        jaVeio.add(a.telefone);
        const noCrm = db.prepare(campos + ' WHERE c.telefone = ?').get(a.telefone);
        if (noCrm) rows.push(noCrm);
        else doWhats.push({ id: -Number(a.telefone.slice(-9)), so_whatsapp: 1, da_agenda: 1, nome: a.nome, telefone: a.telefone, foto_url: '', etapa: '', nao_lidas: 0, etiquetas: [] });
      }
      for (const w of await carregarContatosWhats()) {
        if (doWhats.length >= 24) break;
        if (jaVeio.has(w.telefone)) continue;
        if (!(w.chave.includes(alvo) || (dig.length >= 5 && w.telefone.includes(dig)))) continue;
        jaVeio.add(w.telefone);
        const noCrm = db.prepare(campos + ' WHERE c.telefone = ?').get(w.telefone);
        if (noCrm) { noCrm.nome_whatsapp = w.nome; rows.push(noCrm); }
        else doWhats.push({ id: -Number(w.telefone.slice(-9)), so_whatsapp: 1, nome: w.nome, telefone: w.telefone, foto_url: w.foto, etapa: '', nao_lidas: 0, etiquetas: [] });
      }
    }
    const porContato = {};
    for (const e of db.prepare('SELECT ce.contato_id, e.* FROM etiquetas e JOIN contato_etiquetas ce ON ce.etiqueta_id = e.id').all()) {
      (porContato[e.contato_id] = porContato[e.contato_id] || []).push({ id: e.id, nome: e.nome, cor: e.cor });
    }
    for (const row of rows) { row.etiquetas = porContato[row.id] || []; }
    return sendJSON(res, 200, rows.concat(doWhats));
  }

  // ─── API: Delete contato ──────────────────────────────────────────────────
  if (method === 'DELETE' && pathname.match(/^\/api\/contato\/\d+$/)) {
    const id = pathname.split('/').pop();
    db.prepare('DELETE FROM contatos WHERE id = ?').run(id);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Send message via Evolution ──────────────────────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/mensagem$/)) {
    const id = pathname.split('/')[3];
    const body = await parseBody(req);
    if (!body.texto) return sendJSON(res, 400, { error: 'texto required' });
    // Sugestão de retomada com lacuna ("[dia e hora]", "[responder aqui ...]") não pode sair para o cliente.
    if (/\[(dia e hora|responder aqui[^\]]*)\]/i.test(body.texto)) return sendJSON(res, 400, { error: 'A mensagem ainda tem um trecho entre [colchetes] para preencher.' });
    const contato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Contato not found' });

    // Responder citando uma mensagem: só dá para citar o que tem o identificador do WhatsApp guardado.
    let quoted = null, citada = null;
    if (body.citar) {
      citada = db.prepare('SELECT id, direcao, texto, tipo, wa_msg_id, wa_remote_jid FROM mensagens WHERE id = ? AND contato_id = ?').get(body.citar, id);
      if (citada && citada.wa_msg_id) {
        quoted = { key: { id: citada.wa_msg_id, remoteJid: citada.wa_remote_jid || jidFromPhone(contato.telefone), fromMe: citada.direcao === 'out' }, message: { conversation: resumoParaCitar(citada) } };
      } else { citada = null; }
    }

    // Send via Evolution
    let waKey = null;
    try {
      const result = await evoSendText(contato.telefone, body.texto, quoted);
      if (result && result.key) waKey = result.key;
    } catch (e) {
      console.error('[CRM] Erro enviando msg:', e.message);
      return sendJSON(res, 500, { error: 'Falha ao enviar: ' + e.message });
    }

    // Save message
    const waMsgId = (waKey && waKey.id) || '';
    const waJid = (waKey && waKey.remoteJid) || jidFromPhone(contato.telefone);
    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, wa_msg_id, wa_remote_jid, citacao_texto, citacao_de) VALUES (?, 'out', ?, 'text', ?, ?, ?, ?)").run(
      id, body.texto, waMsgId, waJid, citada ? resumoParaCitar(citada).slice(0, 300) : '', citada ? citada.direcao : '');
    db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(id);
    // Mensagem enviada pela tela do CRM a um orçamento parado também conta como retomada.
    try { retomada.registrarSeRetomada(db, Number(id), body.texto, 'text'); } catch (e) { console.error('[RETOMADA]', e.message); }
    return sendJSON(res, 200, { ok: true, sem_citacao: !!body.citar && !citada });
  }

  // ─── Enviar cartão de contato (um ou vários) para a conversa ────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/cartao$/)) {
    const id = pathname.split('/')[3];
    const body = await parseBody(req);
    const contato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Contato not found' });
    const lista = (Array.isArray(body.contatos) ? body.contatos : [])
      .map(c => ({ nome: String((c && c.nome) || '').trim().slice(0, 80), telefone: String((c && c.telefone) || '').replace(/\D/g, '') }))
      .filter(c => c.telefone.length >= 8).slice(0, 10);
    if (!lista.length) return sendJSON(res, 400, { error: 'Escolha pelo menos um contato com telefone.' });
    let chave = {};
    try {
      const r = await enviarCartao(contato.telefone, lista);
      chave = (r && r.key) || {};
    } catch (e) {
      console.error('[CRM] Erro enviando cartão de contato:', e.message);
      return sendJSON(res, 500, { error: 'Falha ao enviar: ' + e.message });
    }
    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, wa_msg_id, wa_remote_jid, cartao) VALUES (?, 'out', ?, 'contact', ?, ?, ?)").run(
      id, 'Contato: ' + lista.map(c => c.nome || telefoneBonito(c.telefone)).join(', '), chave.id || '', chave.remoteJid || jidFromPhone(contato.telefone), JSON.stringify(lista));
    db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(id);
    console.log('[CRM] cartão de contato enviado (' + lista.length + ') para o contato ' + id);
    return sendJSON(res, 200, { ok: true, enviados: lista.length });
  }

  // ─── API: Send media via Evolution ─────────────────────────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/media$/)) {
    const id = pathname.split('/')[3];
    const body = await parseBody(req, 50e6);
    if (!body.base64 || !body.mediatype) return sendJSON(res, 400, { error: 'base64 and mediatype required' });
    const contato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Contato not found' });

    // Send via Evolution
    let mediaKey = null;
    try {
      // o gerador de orçamento manda "data:application/pdf;...;base64,XXXX": a Evolution só aceita a parte XXXX
      const soBase64 = body.base64.includes(',') ? body.base64.split(',').pop() : body.base64;
      const r = await evoSendMedia(contato.telefone, body.mediatype, soBase64, body.caption || '', body.fileName || '', body.mimetype || '');
      if (r && r.key) mediaKey = r.key;
    } catch (e) {
      console.error('[CRM] Erro enviando media:', e.message);
      return sendJSON(res, 500, { error: 'Falha ao enviar: ' + e.message });
    }

    // Save file locally
    const rawBase64 = body.base64.includes(',') ? body.base64.split(',')[1] : body.base64;
    const mime = body.mimetype || '';
    const ext = body.fileName ? path.extname(body.fileName) : (mime.includes('jpeg') || mime.includes('jpg') ? '.jpg' : mime.includes('png') ? '.png' : mime.includes('pdf') ? '.pdf' : '');
    const filename = `${Date.now()}_${contato.id}_out${ext}`;
    fs.writeFileSync(path.join(MEDIA_DIR, filename), Buffer.from(rawBase64, 'base64'));

    // Foto sem legenda não mostra o nome do arquivo embaixo; documento mostra o nome.
    // Documento com legenda: guarda "nome do arquivo" na primeira linha e a legenda embaixo, para a tela mostrar os dois.
    const msgTexto = (body.mediatype === 'image' ? (body.caption || '[image]') : [body.fileName, body.caption].filter(Boolean).join('\n')) || `[${body.mediatype}]`;
    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path, wa_msg_id, wa_remote_jid) VALUES (?, 'out', ?, ?, ?, ?, ?)").run(
      id, msgTexto, body.mediatype === 'image' ? 'image' : 'document', filename,
      (mediaKey && mediaKey.id) || '', (mediaKey && mediaKey.remoteJid) || jidFromPhone(contato.telefone)
    );
    // PDF de orçamento enviado pelo CRM (botão "Enviar via CRM" do gerador): mesma marcação que o webhook faz quando o
    // PDF sai pelo WhatsApp — etapa Orçamento, relógio do follow-up, modalidade e nome do pet tirados do nome do arquivo.
    if (body.mediatype !== 'image' && /or[cç]amento/i.test(String(body.fileName || '') + ' ' + String(body.caption || ''))) {
      const dn = String(body.fileName || '');
      const modalidade = /tomograf/i.test(dn) ? 'TC' : /resson/i.test(dn) ? 'RM' : /radiograf|raio/i.test(dn) ? 'RX' : /ultrass/i.test(dn) ? 'US' : '';
      const petMatch = dn.match(/-\s*([^()]+?)\s*\(/);
      const pet = petMatch ? petMatch[1].trim().replace(/\.pdf$/i, '') : '';
      const atual = db.prepare('SELECT etapa, pet_nome FROM contatos WHERE id = ?').get(id);
      if (atual) {
        // relógio do follow-up e modalidade valem em qualquer etapa; a etapa só muda sozinha para quem está em "novo"
        // (o gerador de orçamento chama /mover logo em seguida, como sempre fez)
        db.prepare("UPDATE contatos SET orcamento_enviado_at = datetime('now','localtime'), orcamento_modalidade = COALESCE(NULLIF(?, ''), orcamento_modalidade), updated_at = datetime('now','localtime') WHERE id = ?").run(modalidade, id);
        if (atual.etapa === 'novo') {
          db.prepare("UPDATE contatos SET etapa = 'orcamento' WHERE id = ?").run(id);
          db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'novo', 'orcamento')").run(id);
        }
        if (pet && !atual.pet_nome) db.prepare('UPDATE contatos SET pet_nome = ? WHERE id = ?').run(pet, id);
        console.log(`[CRM] orçamento enviado pelo CRM: contato ${id} (${modalidade || 'PDF'}${pet ? ' · ' + pet : ''})`);
      }
    }
    db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(id);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── Webhook: Evolution API ───────────────────────────────────────────────
  if (method === 'POST' && pathname === '/webhook/evolution') {
    const body = await parseBody(req);
    console.log('[CRM] Webhook:', JSON.stringify(body).substring(0, 300));

    const event = body.event;

    // Handle messages.update — sync read status from WhatsApp
    if (event === 'messages.update' || event === 'MESSAGES_UPDATE') {
      const updates = Array.isArray(body.data) ? body.data : [body.data || body];
      for (const upd of updates) {
        // fromMe=false + READ = alguém da recepção leu a mensagem do cliente em outro aparelho. Acha a mensagem
        // pelo identificador e marca a conversa como lida ATÉ essa mensagem (o que chegou depois continua não lido).
        if (upd && upd.keyId && upd.fromMe === false && (upd.status === 'READ' || upd.status === 'PLAYED')) {
          const lida = db.prepare("SELECT contato_id, created_at FROM mensagens WHERE wa_msg_id = ? AND direcao = 'in' LIMIT 1").get(String(upd.keyId));
          if (lida) {
            db.prepare("UPDATE contatos SET last_read_at = ?, manual_unread = 0 WHERE id = ? AND COALESCE(NULLIF(last_read_at,''), '1970-01-01') < ?").run(lida.created_at, lida.contato_id, lida.created_at);
          }
          continue;
        }
        const updKey = upd.key || {};
        const updStatus = upd.update?.status;
        // Status 3 = READ (on WhatsApp), 4 = PLAYED (audio)
        if ((updStatus === 3 || updStatus === 4 || updStatus === 'READ' || updStatus === 'PLAYED') && !updKey.fromMe) {
          const updJid = updKey.remoteJid || '';
          if (updJid.includes('@g.us')) continue;
          const updPhone = phoneFromJid(updJid);
          if (updPhone) {
            const contato = db.prepare('SELECT id FROM contatos WHERE telefone = ?').get(updPhone);
            if (contato) {
              db.prepare("UPDATE contatos SET last_read_at = datetime('now','localtime'), manual_unread = 0 WHERE id = ?").run(contato.id);
              console.log(`[CRM] WhatsApp read sync: ${updPhone} marked as read`);
            }
          }
        }
      }
      return sendJSON(res, 200, { ok: true });
    }

    // send.message = mensagem enviada pela API da Evolution (a Athena, à noite).
    // Essas NÃO vêm como messages.upsert, então o CRM nunca via as respostas
    // dela. Só aceitamos as que levam a marca da Athena: o que o próprio CRM
    // envia já é gravado na hora pela rota /api/contato/:id/mensagem|media e
    // entraria duplicado (26/10/2026).
    const ehSendMessage = (event === 'send.message' || event === 'SEND_MESSAGE');
    if (ehSendMessage) {
      const sm = (body.data || body).message || {};
      const smTexto = sm.conversation || (sm.extendedTextMessage && sm.extendedTextMessage.text) || '';
      if (!smTexto.includes(followup.ATHENA_MARCA)) {
        return sendJSON(res, 200, { ok: true, ignored: true, motivo: 'send.message sem marca da Athena' });
      }
    } else if (event !== 'messages.upsert' && event !== 'MESSAGES_UPSERT') {
      console.log(`[CRM] Non-upsert event: ${event}`);
      return sendJSON(res, 200, { ok: true, ignored: true });
    }

    const data = body.data || body;
    const key = data.key || {};
    const jid = key.remoteJid || '';

    // Mesma mensagem chegando duas vezes (ex.: send.message + upsert) não grava de novo
    if (key.id && db.prepare('SELECT 1 FROM mensagens WHERE wa_msg_id = ? LIMIT 1').get(key.id)) {
      return sendJSON(res, 200, { ok: true, ignored: true, motivo: 'wa_msg_id repetido' });
    }

    // Reação (👍, ❤️…) a uma mensagem: não é mensagem nova. Fica guardada na mensagem reagida, sem mexer na
    // ordem da lista nem no "não lida". key.fromMe diz quem reagiu; reactionMessage.key aponta a mensagem.
    const reacao = data.message && data.message.reactionMessage;
    if (reacao) {
      if (!jid.includes('@g.us') && reacao.key && reacao.key.id) {
        const achou = gravarReacao(reacao.key.id, key.fromMe ? 'out' : 'in', String(reacao.text || ''));
        console.log(`[CRM] reação ${key.fromMe ? 'da recepção' : 'do cliente'}: ${reacao.text || '(tirou)'}${achou ? '' : ' (mensagem não encontrada)'}`);
      }
      return sendJSON(res, 200, { ok: true, reacao: true });
    }

    // Handle group messages
    if (jid.includes('@g.us')) {
      const grupo = db.prepare('SELECT * FROM grupos WHERE jid = ? AND ativo = 1').get(jid);
      if (!grupo) return sendJSON(res, 200, { ok: true, ignored: true });

      const msg = data.message || {};
      const texto = msg.conversation || (msg.extendedTextMessage && msg.extendedTextMessage.text) || '';
      const tipo = msg.audioMessage ? 'audio' : msg.imageMessage ? 'image' : msg.documentMessage ? 'document' : 'text';
      const remetente = key.participant || data.pushName || '';
      const remetenteNome = data.pushName || '';

      if (texto || tipo !== 'text') {
        db.prepare("INSERT INTO grupo_mensagens (grupo_id, remetente, remetente_nome, texto, tipo) VALUES (?, ?, ?, ?, ?)").run(
          grupo.id, remetente, remetenteNome, texto || `[${tipo}]`, tipo
        );
      }
      return sendJSON(res, 200, { ok: true });
    }

    const isFromMe = !!key.fromMe;

    const phone = phoneFromJid(jid);
    if (!phone) return sendJSON(res, 200, { ok: true, ignored: true });

    // ─── Comandos do follow-up ─────────────────────────────────────────────
    // A recepção responde na conversa "Prevenvets" (o número que manda os
    // alertas). Pra instância da recepção isso é uma mensagem ENVIADA pro
    // prevenvet — e cai aqui. Ex.: "perdeu 123", "ok 123", "fechou 123".
    if (isFromMe && phone.replace(/\D/g, '').endsWith(alerta.ORIGEM.slice(-11))) {
      const msgCmd = data.message || {};
      const txt = (msgCmd.conversation || (msgCmd.extendedTextMessage && msgCmd.extendedTextMessage.text) || '').trim();
      const m = txt.match(/^(marcou|desistiu|desistiram|perdeu|perdido|ok|feito|resolvido|fechou|agendou|ignorar|ajuda|\?)\s*#?(\d+)?\s*$/i);
      if (m) {
        const resposta = followup.executarComando(db, m[1].toLowerCase(), m[2] ? Number(m[2]) : null);
        if (resposta) alerta.enviarRecepcao(resposta);
        return sendJSON(res, 200, { ok: true, comando: m[1] });
      }
    }

    // Handle status updates (no actual message content)
    const status = data.status || '';
    if (!data.message && (status === 'SERVER_ACK' || status === 'DELIVERY_ACK' || status === 'READ' || status === 'PLAYED')) {
      // If I read a message on real WhatsApp, mark as read in CRM too
      if (status === 'READ' && !isFromMe) {
        const contato = db.prepare('SELECT id FROM contatos WHERE telefone = ?').get(phone);
        if (contato) {
          db.prepare("UPDATE contatos SET last_read_at = datetime('now','localtime'), manual_unread = 0 WHERE id = ?").run(contato.id);
          console.log(`[CRM] WhatsApp read sync: ${phone} marked as read`);
        }
      }
      return sendJSON(res, 200, { ok: true, ignored: true });
    }

    // Extract message text
    const msg = data.message || {};
    // cartão de contato (um ou vários) vira mensagem do tipo 'contact', com os contatos guardados em "cartao"
    const cartoes = cartoesDaMensagem(msg);
    const texto = msg.conversation || (msg.extendedTextMessage && msg.extendedTextMessage.text) ||
      (cartoes.length ? 'Contato: ' + cartoes.map(c => c.nome || telefoneBonito(c.telefone)).join(', ') : '');
    console.log(`[CRM] Msg ${isFromMe ? 'OUT' : 'IN'} ${phone}: texto="${texto.substring(0,50)}" tipo=${Object.keys(msg).join(',')}`);
    const tipo = msg.audioMessage ? 'audio' : msg.imageMessage ? 'image' : msg.documentMessage ? 'document' : cartoes.length ? 'contact' : 'text';

    // Extract push name (contact name from WhatsApp)
    const pushName = data.pushName || '';

    // Upsert contato
    let contato = db.prepare('SELECT * FROM contatos WHERE telefone = ?').get(phone);
    if (!contato) {
      if (isFromMe) {
        // Create contact from outgoing message (sent via native WhatsApp)
        const result = db.prepare(`
          INSERT INTO contatos (nome, telefone, etapa, last_message_at, updated_at)
          VALUES (?, ?, 'novo', datetime('now','localtime'), datetime('now','localtime'))
        `).run(nomeDaAgenda(phone), phone);
        contato = { id: result.lastInsertRowid, etapa: 'novo' };
        db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, '', 'novo')").run(contato.id);
        console.log(`[CRM] Novo contato (fromMe): ${phone}`);
      } else {
        const fotoUrl = await evoFetchProfilePic(phone);
        const result = db.prepare(`
          INSERT INTO contatos (nome, telefone, etapa, foto_url, last_message_at, updated_at)
          VALUES (?, ?, 'novo', ?, datetime('now','localtime'), datetime('now','localtime'))
        `).run(nomeDaAgenda(phone) || pushName, phone, fotoUrl);
        contato = { id: result.lastInsertRowid, etapa: 'novo' };
        db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, '', 'novo')").run(contato.id);
        console.log(`[CRM] Novo contato: ${pushName} (${phone}) foto: ${fotoUrl ? 'sim' : 'nao'}`);
      }
    } else {
      // Update name if we got a pushName and contact has no name
      if (pushName && !contato.nome && !isFromMe) {
        db.prepare('UPDATE contatos SET nome = ? WHERE id = ?').run(pushName, contato.id);
      }
      // If contact was finalized for 2+ days and THEY sent a message, move back to "novo"
      if (!isFromMe && contato.etapa === 'finalizado' && contato.updated_at < new Date(Date.now() - 2 * 86400000).toISOString().replace('T', ' ').slice(0, 19)) {
        db.prepare("UPDATE contatos SET etapa = 'novo', last_message_at = datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE id = ?").run(contato.id);
        db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'finalizado', 'novo')").run(contato.id);
        console.log(`[CRM] Contato ${contato.nome || phone} voltou de finalizado para novo`);
      } else {
        db.prepare("UPDATE contatos SET last_message_at = datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE id = ?").run(contato.id);
      }
      // If I'm sending a message, mark as read (I must have read the conversation)
      if (isFromMe) {
        db.prepare("UPDATE contatos SET last_read_at = datetime('now','localtime'), manual_unread = 0 WHERE id = ?").run(contato.id);
        console.log(`[CRM] fromMe read sync: ${phone} marked as read`);
      }
    }

    // Save incoming message + download media
    if (texto || tipo !== 'text') {
      let mediaPath = '';
      if (tipo !== 'text' && tipo !== 'contact') {
        try {
          const mediaData = await evoDownloadMedia(key);
          if (mediaData && mediaData.base64) {
            const mime = mediaData.mimetype || '';
            const ext = mime.includes('jpeg') || mime.includes('jpg') ? '.jpg'
              : mime.includes('png') ? '.png'
              : mime.includes('webp') ? '.webp'
              : mime.includes('mp4') ? '.mp4'
              : mime.includes('ogg') || mime.includes('opus') ? '.ogg'
              : mime.includes('pdf') ? '.pdf'
              : mime.includes('mp3') ? '.mp3'
              : '';
            const filename = `${Date.now()}_${contato.id}${ext}`;
            fs.writeFileSync(path.join(MEDIA_DIR, filename), Buffer.from(mediaData.base64, 'base64'));
            mediaPath = filename;
            console.log(`[CRM] Media salva: ${filename} (${tipo})`);
          }
        } catch (e) { console.error('[CRM] Erro download media:', e.message); }
      }

      const caption = tipo === 'image' && msg.imageMessage && msg.imageMessage.caption ? msg.imageMessage.caption : '';
      const direcao = isFromMe ? 'out' : 'in';
      const waMsgId = (key && key.id) || '';
      const waJid = jid || '';
      const docName = (tipo === 'document' && msg.documentMessage && msg.documentMessage.fileName) ? String(msg.documentMessage.fileName) : '';
      const docCaption = (tipo === 'document' && msg.documentMessage && msg.documentMessage.caption) ? String(msg.documentMessage.caption) : '';
      const cita = citacaoDoContexto(msg, data, jid);
      db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path, wa_msg_id, wa_remote_jid, citacao_texto, citacao_de) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").run(
        contato.id, direcao, texto || caption || [docName, docCaption].filter(Boolean).join('\n') || `[${tipo}]`, tipo, mediaPath, waMsgId, waJid, cita.texto, cita.de
      );
      if (cartoes.length && waMsgId) db.prepare('UPDATE mensagens SET cartao = ? WHERE wa_msg_id = ?').run(JSON.stringify(cartoes), waMsgId);
      // mensagem com link (pedido no Adobe, LoopVet…): busca a prévia em segundo plano
      if (tipo === 'text' && waMsgId && /https?:\/\//.test(texto)) {
        try { const nv = db.prepare('SELECT id FROM mensagens WHERE wa_msg_id = ?').get(waMsgId); if (nv) agendarPrevia(nv.id, texto); } catch (e) {}
      }
      // De onde o cliente veio (anúncio Meta, Google, direto) — só leitura das primeiras mensagens dele.
      if (!isFromMe) { try { origem.classificar(db, contato.id, texto || caption); } catch (e) { console.error('[ORIGEM]', e.message); } }
      // A recepção escreveu para um orçamento parado (🟡/⚪)? Conta como retomada (1ª, 2ª ou última).
      else { try { retomada.registrarSeRetomada(db, contato.id, texto || caption || docName || docCaption, tipo); } catch (e) { console.error('[RETOMADA]', e.message); } }
      // Cliente mandou foto, PDF ou áudio: lê/transcreve em seguida (pedido do veterinário, comprovante, fala). Só transcreve.
      if (!isFromMe && mediaPath && (tipo === 'image' || tipo === 'document' || tipo === 'audio')) { try { leitor.aoReceber(db); } catch (e) { console.error('[LEITOR]', e.message); } }
    }

    // ─── Auto-move to Orçamento when PDF sent outgoing ───────────────────
    if (isFromMe && tipo === 'document') {
      // É orçamento mesmo? O gerador nomeia "Orçamento Tomografia - Pet (Tutor).pdf".
      // Se o nome não disser, vale o que o time escreveu nos últimos 10 min
      // ("segue o orçamento..."). Nota fiscal pro contador NÃO é orçamento.
      const dn = (msg.documentMessage && msg.documentMessage.fileName) ? String(msg.documentMessage.fileName) : '';
      const dc = (msg.documentMessage && msg.documentMessage.caption) ? String(msg.documentMessage.caption) : '';
      const reOrc = /or[cç]amento/i;
      const textoRecente = db.prepare("SELECT texto FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND tipo = 'text' AND created_at >= datetime('now','localtime','-10 minutes') ORDER BY id DESC LIMIT 3").all(contato.id).map(r => r.texto).filter(t => !String(t).includes(followup.ATHENA_MARCA)).join(' ');
      const ehOrcamento = reOrc.test(dn) || reOrc.test(dc) || reOrc.test(textoRecente);

      // Modalidade e pet saem do nome do arquivo, quando houver.
      const modalidade = /tomograf/i.test(dn) ? 'TC' : /resson/i.test(dn) ? 'RM' : /radiograf|raio/i.test(dn) ? 'RX' : /ultrass/i.test(dn) ? 'US' : '';
      const petMatch = dn.match(/-\s*([^()]+?)\s*\(/);
      const petDoArquivo = petMatch ? petMatch[1].trim().replace(/\.pdf$/i, '') : '';

      // Só 'novo'. Quem está em 'agendado' recebe PDF de LAUDO, não de orçamento.
      const etapasAntes = ['novo'];
      const currentEtapa = db.prepare('SELECT etapa, pet_nome FROM contatos WHERE id = ?').get(contato.id);
      if (!ehOrcamento) {
        console.log(`[CRM] PDF ignorado (não é orçamento): ${contato.nome || phone} "${dn.slice(0, 40)}"`);
      } else if (currentEtapa && etapasAntes.includes(currentEtapa.etapa)) {
        db.prepare("UPDATE contatos SET etapa = 'orcamento', orcamento_enviado_at = datetime('now','localtime'), orcamento_modalidade = ?, updated_at = datetime('now','localtime') WHERE id = ?").run(modalidade, contato.id);
        if (petDoArquivo && !currentEtapa.pet_nome) db.prepare("UPDATE contatos SET pet_nome = ? WHERE id = ?").run(petDoArquivo, contato.id);
        db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'orcamento')").run(contato.id, currentEtapa.etapa);
        console.log(`[CRM] Auto-move para Orçamento: ${contato.nome || phone} (${modalidade || 'PDF'}${petDoArquivo ? ' · ' + petDoArquivo : ''})`);
      } else if (currentEtapa && currentEtapa.etapa === 'orcamento') {
        // Orçamento novo pra quem já estava em orçamento: renova o relógio e a modalidade.
        db.prepare("UPDATE contatos SET orcamento_enviado_at = datetime('now','localtime'), orcamento_modalidade = COALESCE(NULLIF(?, ''), orcamento_modalidade), updated_at = datetime('now','localtime') WHERE id = ?").run(modalidade, contato.id);
        if (petDoArquivo && !currentEtapa.pet_nome) db.prepare("UPDATE contatos SET pet_nome = ? WHERE id = ?").run(petDoArquivo, contato.id);
      } else if (currentEtapa && currentEtapa.etapa === 'orcamento' && !db.prepare('SELECT orcamento_enviado_at FROM contatos WHERE id = ?').get(contato.id).orcamento_enviado_at) {
        // Already in orcamento but no timestamp — set it now
        db.prepare("UPDATE contatos SET orcamento_enviado_at = datetime('now','localtime') WHERE id = ?").run(contato.id);
      }
    }

    // ─── Auto-move to Agendado when scheduling message sent ──────────────
    if (isFromMe && tipo === 'text' && texto && !texto.includes(followup.ATHENA_MARCA)) {
      const lower = texto.toLowerCase();
      const agendaKeywords = ['agendado', 'agendamos', 'agendada', 'confirmado para', 'confirmada para', 'marcado para', 'marcada para', 'exame agendado', 'exame marcado'];
      // "marcado" / "reservado" / "confirmado" secos também fecham (confirmação curta do time).
      const hasAgenda = agendaKeywords.some(kw => lower.includes(kw)) || followup.ehConfirmacaoCurta(texto);
      if (hasAgenda) {
        const currentEtapa = db.prepare('SELECT etapa FROM contatos WHERE id = ?').get(contato.id);
        const etapasAntes = ['novo', 'orcamento'];
        if (currentEtapa && etapasAntes.includes(currentEtapa.etapa)) {
          db.prepare("UPDATE contatos SET etapa = 'agendado', updated_at = datetime('now','localtime') WHERE id = ?").run(contato.id);
          db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'agendado')").run(contato.id, currentEtapa.etapa);
          console.log(`[CRM] Auto-move para Agendado: ${contato.nome || phone} (mensagem: "${texto.substring(0, 40)}")`);
        }
        // Extrair nome do animal entre parênteses na mensagem de agendamento
        const parenMatch = texto.match(/\(([^)]+)\)/);
        if (parenMatch) {
          const petNome = parenMatch[1].trim();
          db.prepare("UPDATE contatos SET pet_nome = ?, updated_at = datetime('now','localtime') WHERE id = ?").run(petNome, contato.id);
          console.log(`[CRM] Pet nome extraído: "${petNome}" para ${contato.nome || phone}`);
        }
      }
    }

    // ─── Track last client response (for follow-up) ─────────────────────
    if (!isFromMe) {
      db.prepare("UPDATE contatos SET ultima_resposta_cliente_at = datetime('now','localtime') WHERE id = ?").run(contato.id);
    }

    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Grupos ───────────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/grupos') {
    const grupos = db.prepare(`
      SELECT g.*,
        (SELECT COUNT(*) FROM grupo_mensagens WHERE grupo_id = g.id) as total_msgs,
        (SELECT texto FROM grupo_mensagens WHERE grupo_id = g.id ORDER BY created_at DESC LIMIT 1) as ultima_msg,
        (SELECT remetente_nome FROM grupo_mensagens WHERE grupo_id = g.id ORDER BY created_at DESC LIMIT 1) as ultimo_remetente,
        (SELECT created_at FROM grupo_mensagens WHERE grupo_id = g.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_at,
        (SELECT COUNT(*) FROM grupo_mensagens WHERE grupo_id = g.id AND remetente != 'me' AND created_at > COALESCE(NULLIF(g.last_read_at,''), '1970-01-01')) as nao_lidas
      FROM grupos g WHERE g.ativo = 1 ORDER BY g.nome
    `).all();
    return sendJSON(res, 200, grupos);
  }

  if (method === 'GET' && pathname.match(/^\/api\/grupo\/\d+\/mensagens$/)) {
    const id = pathname.split('/')[3];
    const msgs = db.prepare('SELECT * FROM grupo_mensagens WHERE grupo_id = ? ORDER BY created_at DESC LIMIT 100').all(id);
    db.prepare("UPDATE grupos SET last_read_at = datetime('now','localtime') WHERE id = ?").run(id);
    return sendJSON(res, 200, msgs);
  }

  // List available WhatsApp groups from Evolution API
  if (method === 'GET' && pathname === '/api/grupos/disponiveis') {
    try {
      const data = await new Promise((resolve, reject) => {
        const url = new URL(`${EVO_BASE}/group/fetchAllGroups/${EVO_INSTANCE}?getParticipants=false`);
        http.get({ hostname: url.hostname, port: url.port, path: url.pathname + url.search, headers: { apikey: EVO_KEY } }, res => {
          let d = ''; res.on('data', c => d += c);
          res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve([]); } });
        }).on('error', () => resolve([]));
      });
      const groups = (Array.isArray(data) ? data : []).map(g => ({ jid: g.id, nome: g.subject }));
      // Mark which are already being monitored
      const monitored = db.prepare('SELECT jid FROM grupos WHERE ativo = 1').all().map(g => g.jid);
      groups.forEach(g => { g.monitorando = monitored.includes(g.jid); });
      return sendJSON(res, 200, groups);
    } catch (e) { return sendJSON(res, 200, []); }
  }

  if (method === 'POST' && pathname === '/api/grupo') {
    const body = await parseBody(req);
    if (!body.jid || !body.nome) return sendJSON(res, 400, { error: 'jid and nome required' });
    try {
      const fotoUrl = await evoFetchProfilePic(body.jid);
      db.prepare('INSERT OR IGNORE INTO grupos (jid, nome, foto_url) VALUES (?, ?, ?)').run(body.jid, body.nome, fotoUrl || '');
      return sendJSON(res, 201, { ok: true });
    } catch (e) { return sendJSON(res, 409, { error: e.message }); }
  }

  if (method === 'DELETE' && pathname.match(/^\/api\/grupo\/\d+$/)) {
    const id = pathname.split('/').pop();
    db.prepare('UPDATE grupos SET ativo = 0 WHERE id = ?').run(id);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Send message to group ─────────────────────────────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/grupo\/\d+\/mensagem$/)) {
    const id = pathname.split('/')[3];
    const body = await parseBody(req);
    if (!body.texto) return sendJSON(res, 400, { error: 'texto required' });
    const grupo = db.prepare('SELECT * FROM grupos WHERE id = ?').get(id);
    if (!grupo) return sendJSON(res, 404, { error: 'Grupo not found' });

    try {
      await evoSendText(grupo.jid, body.texto);
    } catch (e) {
      console.error('[CRM] Erro enviando msg grupo:', e.message);
      return sendJSON(res, 500, { error: 'Falha ao enviar: ' + e.message });
    }

    db.prepare("INSERT INTO grupo_mensagens (grupo_id, remetente, remetente_nome, texto, tipo) VALUES (?, 'me', 'Eu', ?, 'text')").run(id, body.texto);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Team members ─────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/equipe') {
    if (!authDb) return sendJSON(res, 200, []);
    try {
      const members = authDb.prepare("SELECT id, username, name FROM users WHERE active = 1 ORDER BY name").all();
      return sendJSON(res, 200, members);
    } catch (e) {
      console.error('[CRM] Erro lendo equipe:', e.message);
      return sendJSON(res, 200, []);
    }
  }

  // ─── API: Custom Tabs (abas) ────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/abas') {
    const abas = db.prepare('SELECT * FROM abas_custom WHERE ativo = 1 ORDER BY ordem, id').all();
    // Add unread count per tab
    for (const aba of abas) {
      aba.nao_lidas = db.prepare(`
        SELECT COUNT(*) as c FROM mensagens m
        JOIN contato_abas ca ON ca.contato_id = m.contato_id
        JOIN contatos ct ON ct.id = m.contato_id
        WHERE ca.aba_id = ? AND m.direcao = 'in'
        AND m.created_at > COALESCE(NULLIF(ct.last_read_at,''), '1970-01-01')
      `).get(aba.id).c;
    }
    return sendJSON(res, 200, abas);
  }

  if (method === 'POST' && pathname === '/api/abas') {
    const body = await parseBody(req);
    if (!body.nome) return sendJSON(res, 400, { error: 'nome required' });
    const maxOrdem = db.prepare('SELECT COALESCE(MAX(ordem),0) as m FROM abas_custom').get().m;
    const result = db.prepare('INSERT INTO abas_custom (nome, icone, cor, ordem) VALUES (?, ?, ?, ?)').run(body.nome, body.icone || '📋', body.cor || '#283593', maxOrdem + 1);
    return sendJSON(res, 201, { ok: true, id: result.lastInsertRowid });
  }

  if (method === 'DELETE' && pathname.match(/^\/api\/aba\/\d+$/)) {
    const id = pathname.split('/').pop();
    db.prepare('UPDATE abas_custom SET ativo = 0 WHERE id = ?').run(id);
    db.prepare('DELETE FROM contato_abas WHERE aba_id = ?').run(id);
    return sendJSON(res, 200, { ok: true });
  }

  // Get contacts for a specific tab
  if (method === 'GET' && pathname.match(/^\/api\/aba\/\d+\/contatos$/)) {
    const abaId = pathname.split('/')[3];
    // Check if this is the "Favoritos" tab — use favorito field instead of contato_abas
    const aba = db.prepare('SELECT nome FROM abas_custom WHERE id = ?').get(abaId);
    let rows;
    if (aba && aba.nome === 'Favoritos') {
      rows = db.prepare(`
        SELECT c.*, c.pinado,
          (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id) as total_msgs,
          (SELECT texto FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg,
          (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id AND direcao = 'in' AND created_at > COALESCE(NULLIF(c.last_read_at,''), '1970-01-01')) as nao_lidas,
          (SELECT direcao FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_dir,
          (SELECT created_at FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_at
        FROM contatos c
        WHERE c.favorito = 1 ORDER BY c.pinado DESC, c.last_message_at DESC
      `).all();
    } else {
      rows = db.prepare(`
        SELECT c.*, ca.pinado,
          (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id) as total_msgs,
          (SELECT texto FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg,
          (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id AND direcao = 'in' AND created_at > COALESCE(NULLIF(c.last_read_at,''), '1970-01-01')) as nao_lidas,
          (SELECT direcao FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_dir,
          (SELECT created_at FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_at
        FROM contatos c
        JOIN contato_abas ca ON ca.contato_id = c.id
        WHERE ca.aba_id = ? ORDER BY ca.pinado DESC, c.last_message_at DESC
      `).all(abaId);
    }
    const getEtiquetas = db.prepare('SELECT e.* FROM etiquetas e JOIN contato_etiquetas ce ON ce.etiqueta_id = e.id WHERE ce.contato_id = ?');
    for (const row of rows) { row.etiquetas = getEtiquetas.all(row.id); }
    return sendJSON(res, 200, rows);
  }

  // Add/remove contact from tab
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/aba\/\d+$/)) {
    const parts = pathname.split('/');
    const contatoId = parts[3], abaId = parts[5];
    db.prepare('INSERT OR IGNORE INTO contato_abas (contato_id, aba_id) VALUES (?, ?)').run(contatoId, abaId);
    return sendJSON(res, 200, { ok: true });
  }

  if (method === 'DELETE' && pathname.match(/^\/api\/contato\/\d+\/aba\/\d+$/)) {
    const parts = pathname.split('/');
    const contatoId = parts[3], abaId = parts[5];
    db.prepare('DELETE FROM contato_abas WHERE contato_id = ? AND aba_id = ?').run(contatoId, abaId);
    return sendJSON(res, 200, { ok: true });
  }

  // Toggle pin on contact in tab
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/aba\/\d+\/pin$/)) {
    const parts = pathname.split('/');
    const contatoId = parts[3], abaId = parts[5];
    const current = db.prepare('SELECT pinado FROM contato_abas WHERE contato_id = ? AND aba_id = ?').get(contatoId, abaId);
    if (!current) return sendJSON(res, 404, { error: 'Not found' });
    const newVal = current.pinado ? 0 : 1;
    db.prepare('UPDATE contato_abas SET pinado = ? WHERE contato_id = ? AND aba_id = ?').run(newVal, contatoId, abaId);
    return sendJSON(res, 200, { ok: true, pinado: newVal });
  }

  // Get which tabs a contact belongs to
  if (method === 'GET' && pathname.match(/^\/api\/contato\/\d+\/abas$/)) {
    const contatoId = pathname.split('/')[3];
    const abas = db.prepare('SELECT aba_id FROM contato_abas WHERE contato_id = ?').all(contatoId);
    return sendJSON(res, 200, abas.map(a => a.aba_id));
  }

  if (method === 'GET' && pathname === '/api/contatos/buscar') {
    const q = new URL(req.url, 'http://localhost').searchParams.get('q') || '';
    if (q.length < 2) return sendJSON(res, 200, []);
    const rows = db.prepare("SELECT id, nome, telefone, foto_url, equipe_xr FROM contatos WHERE nome LIKE ? OR telefone LIKE ? LIMIT 20").all(`%${q}%`, `%${q}%`);
    return sendJSON(res, 200, rows);
  }

  // ─── API: Refresh profile pics ─────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/refresh-fotos') {
    const contatos = db.prepare("SELECT id, telefone FROM contatos WHERE foto_url IS NULL OR foto_url = ''").all();
    let updated = 0;
    for (const c of contatos) {
      const url = await evoFetchProfilePic(c.telefone);
      if (url) {
        db.prepare('UPDATE contatos SET foto_url = ? WHERE id = ?').run(url, c.id);
        updated++;
      }
    }
    return sendJSON(res, 200, { ok: true, updated, total: contatos.length });
  }

  // ─── API: Refresh group pics ──────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/refresh-fotos-grupos') {
    const grupos = db.prepare("SELECT id, jid FROM grupos WHERE ativo = 1 AND (foto_url IS NULL OR foto_url = '')").all();
    let updated = 0;
    for (const g of grupos) {
      const url = await evoFetchProfilePic(g.jid);
      if (url) {
        db.prepare('UPDATE grupos SET foto_url = ? WHERE id = ?').run(url, g.id);
        updated++;
      }
    }
    return sendJSON(res, 200, { ok: true, updated, total: grupos.length });
  }

  // ─── API: Tab badge counts ────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/badges') {
    const gruposNaoLidas = db.prepare(`
      SELECT COALESCE(SUM(sub.cnt), 0) as total FROM (
        SELECT COUNT(*) as cnt FROM grupo_mensagens gm
        JOIN grupos g ON g.id = gm.grupo_id
        WHERE g.ativo = 1 AND gm.remetente != 'me'
        AND gm.created_at > COALESCE(NULLIF(g.last_read_at,''), '1970-01-01')
      ) sub
    `).get();
    // Per-tab unread counts
    const abas = db.prepare('SELECT id FROM abas_custom WHERE ativo = 1').all();
    const abasBadges = {};
    for (const aba of abas) {
      abasBadges[aba.id] = db.prepare(`
        SELECT COUNT(*) as c FROM mensagens m
        JOIN contato_abas ca ON ca.contato_id = m.contato_id
        JOIN contatos ct ON ct.id = m.contato_id
        WHERE ca.aba_id = ? AND m.direcao = 'in'
        AND m.created_at > COALESCE(NULLIF(ct.last_read_at,''), '1970-01-01')
      `).get(aba.id).c;
    }
    return sendJSON(res, 200, { grupos: gruposNaoLidas.total, abas: abasBadges });
  }

  // ─── API: Delete message (CRM + WhatsApp) ─────────────────────────────────
  // Editar uma mensagem de texto que a XR enviou (o WhatsApp só permite até 15 minutos depois do envio)
  if (method === 'PUT' && pathname.match(/^\/api\/mensagem\/\d+$/)) {
    const msgId = pathname.split('/').pop();
    const body = await parseBody(req);
    const texto = String(body.texto || '').trim();
    if (!texto) return sendJSON(res, 400, { error: 'A mensagem não pode ficar vazia.' });
    if (/\[(dia e hora|responder aqui[^\]]*)\]/i.test(texto)) return sendJSON(res, 400, { error: 'A mensagem ainda tem um trecho entre [colchetes] para preencher.' });
    const msg = db.prepare('SELECT * FROM mensagens WHERE id = ?').get(msgId);
    if (!msg) return sendJSON(res, 404, { error: 'Mensagem não encontrada' });
    if (msg.direcao !== 'out' || msg.tipo !== 'text' || !msg.wa_msg_id) return sendJSON(res, 400, { error: 'Só dá para editar mensagem de texto enviada pela XR.' });
    const idade = Date.now() - new Date(String(msg.created_at).replace(' ', 'T') + '-03:00').getTime();
    if (!(idade < 15 * 60000)) return sendJSON(res, 400, { error: 'O WhatsApp só deixa editar até 15 minutos depois do envio.' });
    try {
      // endereço da conversa como a Evolution guardou (pode ser o endereço novo do WhatsApp, em LID)
      let jid = msg.wa_remote_jid || '';
      const achada = await evoPost('/chat/findMessages/' + EVO_INSTANCE, { where: { key: { id: msg.wa_msg_id } } });
      const rec = achada.json && achada.json.messages && achada.json.messages.records && achada.json.messages.records[0];
      if (rec && rec.key && rec.key.remoteJid) jid = rec.key.remoteJid;
      if (!jid) { const c = db.prepare('SELECT telefone FROM contatos WHERE id = ?').get(msg.contato_id); jid = jidFromPhone(c ? c.telefone : ''); }
      const r = await evoPost('/chat/updateMessage/' + EVO_INSTANCE, { number: jid, text: texto, key: { remoteJid: jid, fromMe: true, id: msg.wa_msg_id } });
      if (r.status >= 400) {
        const m = r.json && r.json.response && r.json.response.message ? JSON.stringify(r.json.response.message) : (r.json && (r.json.error || r.json.message)) || ('código ' + r.status);
        console.error('[CRM] edição recusada:', String(m).slice(0, 200));
        return sendJSON(res, 400, { error: 'O WhatsApp não aceitou a edição: ' + String(m).slice(0, 160) });
      }
      db.prepare('UPDATE mensagens SET texto = ?, editada = 1 WHERE id = ?').run(texto, msgId);
      console.log('[CRM] mensagem ' + msgId + ' editada');
      return sendJSON(res, 200, { ok: true });
    } catch (e) { console.error('[CRM] erro editando:', e.message); return sendJSON(res, 502, { error: e.message }); }
  }

  // Reagir a uma mensagem (ou tirar a reação, com emoji vazio), como no WhatsApp
  if (method === 'POST' && pathname.match(/^\/api\/mensagem\/\d+\/reacao$/)) {
    const msgId = pathname.split('/')[3];
    const body = await parseBody(req);
    const emoji = String(body.emoji || '').trim().slice(0, 16);
    const msg = db.prepare('SELECT * FROM mensagens WHERE id = ?').get(msgId);
    if (!msg) return sendJSON(res, 404, { error: 'Mensagem não encontrada' });
    if (!msg.wa_msg_id) return sendJSON(res, 400, { error: 'Não dá para reagir a esta mensagem (ela é antiga, de antes de o CRM guardar o identificador do WhatsApp).' });
    try {
      const jid = await jidDaMensagem(msg);
      const r = await evoPost('/message/sendReaction/' + EVO_INSTANCE, { key: { remoteJid: jid, fromMe: msg.direcao === 'out', id: msg.wa_msg_id }, reaction: emoji });
      if (r.status >= 400) {
        const m = r.json && r.json.response && r.json.response.message ? JSON.stringify(r.json.response.message) : (r.json && (r.json.error || r.json.message)) || ('código ' + r.status);
        console.error('[CRM] reação recusada:', String(m).slice(0, 200));
        return sendJSON(res, 400, { error: 'O WhatsApp não aceitou a reação: ' + String(m).slice(0, 160) });
      }
      gravarReacao(msg.wa_msg_id, 'out', emoji);
      console.log('[CRM] reação ' + (emoji || '(tirada)') + ' na mensagem ' + msgId);
      return sendJSON(res, 200, { ok: true });
    } catch (e) { console.error('[CRM] erro reagindo:', e.message); return sendJSON(res, 502, { error: e.message }); }
  }

  if (method === 'DELETE' && pathname.match(/^\/api\/mensagem\/\d+$/)) {
    const msgId = pathname.split('/').pop();
    const msg = db.prepare('SELECT * FROM mensagens WHERE id = ?').get(msgId);
    if (!msg) return sendJSON(res, 404, { error: 'Message not found' });

    // Try to delete from WhatsApp if we have the key
    if (msg.wa_msg_id && msg.wa_remote_jid) {
      try {
        const fromMe = msg.direcao === 'out';
        await evoDeleteMessage(msg.wa_remote_jid, msg.wa_msg_id, fromMe);
        console.log('[CRM] Deleted from WhatsApp:', msg.wa_msg_id);
      } catch (e) {
        console.error('[CRM] Erro deletando do WhatsApp:', e.message);
      }
    }

    // Delete from CRM database
    db.prepare('DELETE FROM mensagens WHERE id = ?').run(msgId);
    console.log('[CRM] Deleted msg ' + msgId);
    return sendJSON(res, 200, { ok: true, whatsappDeleted: !!(msg.wa_msg_id && msg.wa_remote_jid) });
  }

  // ─── API: Mensagens Rápidas CRUD ──────────────────────────────────────────
  // ─── Agenda do CFAZ a partir da conversa ───────────────────────────────────────────────────
  // Horários livres e já marcados de uma agenda num dia (o cálculo de livres é do próprio CFAZ).
  if (method === 'GET' && pathname === '/api/cfaz/agenda') {
    try {
      const calendarios = await cfazCalendarios();
      const cal = Number(urlObj.searchParams.get('cal') || 0);
      const data = urlObj.searchParams.get('data') || '';
      if (!cal || !/^\d{4}-\d{2}-\d{2}$/.test(data)) return sendJSON(res, 200, { calendarios, livres: [], marcados: [] });
      const [slots, eventos] = await Promise.all([cfaz('GET', '/api/v1/calendars/' + cal + '/available_slots.json?date=' + data), cfazEventos()]);
      const livres = slots.status === 200 && slots.json && Array.isArray(slots.json.available_slots) ? slots.json.available_slots.map(s => s.start_hour) : null;
      const marcados = eventos.filter(e => e.calendar_id === cal && String(e.start_date || '').startsWith(data))
        .sort((a, b) => String(a.start_date).localeCompare(String(b.start_date)))
        .map(e => ({ hora: String(e.start_date).slice(11, 16), fim: String(e.end_date || '').slice(11, 16), titulo: String(e.title || '').slice(0, 40) }));
      return sendJSON(res, 200, { calendarios, livres: livres || [], marcados, aviso: livres ? '' : 'O CFAZ não informou os horários livres deste dia.' });
    } catch (e) { return sendJSON(res, 502, { error: e.message }); }
  }

  // Cria a marcação no CFAZ. Nada é criado sem alguém apertar "Criar marcação" na tela.
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/agenda-cfaz$/)) {
    const id = Number(pathname.split('/')[3]);
    const body = await parseBody(req);
    const contato = db.prepare('SELECT id, nome, telefone, etapa FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Contato não encontrado' });
    const cal = Number(body.cal || 0), data = String(body.data || ''), hora = String(body.hora || ''), titulo = String(body.titulo || '').trim();
    if (!cal || !/^\d{4}-\d{2}-\d{2}$/.test(data) || !/^\d{2}:\d{2}$/.test(hora) || !titulo) return sendJSON(res, 400, { error: 'Informe agenda, dia, horário e título.' });
    // a mesma mensagem pode gerar marcações em agendas diferentes; só não repete a mesma agenda no mesmo horário
    if (body.msg_id && db.prepare('SELECT 1 FROM agenda_cfaz WHERE msg_id = ? AND calendario_id = ? AND inicio = ?').get(body.msg_id, cal, data + ' ' + hora)) return sendJSON(res, 400, { error: 'Esta marcação já foi criada a partir desta mensagem.' });
    try {
      const cals = await cfazCalendarios();
      const c = cals.find(x => x.id === cal);
      if (!c) return sendJSON(res, 400, { error: 'Essa agenda não está ligada no CFAZ.' });
      // O CFAZ aceita duas marcações no mesmo horário sem reclamar (visto no teste de 11/11/2026). A trava fica aqui:
      // se já existe marcação nesse horário e nessa agenda, só cria quando a pessoa confirmar na tela (forcar).
      const ini = new Date(data + 'T' + hora + ':00-03:00');
      if (!body.forcar) {
        // bate se o horário novo cai dentro de uma marcação existente ou encosta nela (cada uma dura o tempo padrão da agenda)
        const fimNovo = ini.getTime() + (c.duracao || 20) * 60000;
        const jaTem = (await cfazEventos(true)).find(e => {
          if (e.calendar_id !== cal) return false;
          const a = new Date(e.start_date).getTime(), b = e.end_date ? new Date(e.end_date).getTime() : a + 20 * 60000;
          return a < fimNovo && b > ini.getTime();
        });
        if (jaTem) return sendJSON(res, 409, { error: 'Já existe uma marcação das ' + String(jaTem.start_date).slice(11, 16) + ' às ' + String(jaTem.end_date || '').slice(11, 16) + ' nessa agenda (' + String(jaTem.title || '').slice(0, 40) + ').' });
      }
      const fim = new Date(ini.getTime() + (c.duracao || 20) * 60000);
      const iso = d => new Date(d.getTime() - 3 * 3600000).toISOString().replace('Z', '-03:00');
      // send_reminder false: igual às marcações que a recepção cria hoje; o CFAZ não manda lembrete a ninguém.
      const evento = { calendar_id: cal, title: titulo.slice(0, 120), description: String(body.descricao || '').slice(0, 300), start_date: iso(ini), end_date: iso(fim), status: 0, send_reminder: false };
      const tel = telefoneCfaz(body.telefone !== undefined ? body.telefone : contato.telefone);
      if (tel) evento.phone = tel;
      const r = await cfaz('POST', '/api/v1/events.json', { event: evento });
      const criado = r.json && (r.json.data || r.json.event || r.json);
      if (!(r.status === 200 || r.status === 201) || !criado || !criado.id) {
        const motivo = r.json && Array.isArray(r.json.errors) ? r.json.errors.join('; ') : ('código ' + r.status);
        console.error('[CFAZ] criação recusada:', motivo);
        return sendJSON(res, 400, { error: 'O CFAZ não aceitou: ' + motivo });
      }
      db.prepare('INSERT OR REPLACE INTO agenda_cfaz (evento_id, contato_id, msg_id, calendario_id, calendario_nome, inicio, titulo) VALUES (?, ?, ?, ?, ?, ?, ?)').run(
        criado.id, id, body.msg_id || null, cal, c.nome, data + ' ' + hora, titulo);
      cfazEvCache.em = 0;
      console.log(`[CFAZ] marcação ${criado.id} criada: ${c.nome} ${data} ${hora} "${titulo}" (contato ${id})`);
      return sendJSON(res, 200, { ok: true, evento_id: criado.id, calendario: c.nome, inicio: data + ' ' + hora });
    } catch (e) { console.error('[CFAZ] erro criando:', e.message); return sendJSON(res, 502, { error: e.message }); }
  }

  // Desfaz uma marcação criada pelo CRM (apaga no CFAZ). Só as que o próprio CRM criou.
  if (method === 'DELETE' && pathname.match(/^\/api\/agenda-cfaz\/\d+$/)) {
    const eventoId = Number(pathname.split('/').pop());
    const row = db.prepare('SELECT * FROM agenda_cfaz WHERE evento_id = ?').get(eventoId);
    if (!row) return sendJSON(res, 404, { error: 'Essa marcação não foi criada pelo CRM.' });
    try {
      const r = await cfaz('DELETE', '/api/v1/events/' + eventoId + '.json');
      if (r.status !== 200 && r.status !== 204 && r.status !== 404) return sendJSON(res, 400, { error: 'O CFAZ não deixou apagar (código ' + r.status + ').' });
      db.prepare('DELETE FROM agenda_cfaz WHERE evento_id = ?').run(eventoId);
      cfazEvCache.em = 0;
      console.log(`[CFAZ] marcação ${eventoId} apagada pelo CRM (contato ${row.contato_id})`);
      return sendJSON(res, 200, { ok: true });
    } catch (e) { return sendJSON(res, 502, { error: e.message }); }
  }

  // Sugestão ao digitar (sem IA): frases idênticas que a recepção mais enviou em 60 dias, a vários clientes.
  // Fica de fora o que tem número (preço, hora, data mudam), nome de tutor ou de animal, e mensagens automáticas.
  if (method === 'GET' && pathname === '/api/frases') {
    const agora = Date.now();
    if (!frasesCache.em || agora - frasesCache.em > 15 * 60 * 1000) {
      const comuns = new Set('bom boa ola olá oi segue poderia temos estamos pode seria gostaria infelizmente precisamos assim irei vou qual como tudo para por me tem sim nao não certo combinado perfeito otimo ótimo obrigado obrigada ok claro isso aguardo aguardamos verifiquei consegue conseguimos realizamos hoje amanha amanhã segunda terca terça quarta quinta sexta sabado sábado clinica clínica pet dra dr vet xr tomografia ressonancia ressonância raio ultrassom exame exames laudo pedido petlove sol flor mel'.split(' '));
      const contagem = {};
      const conta = v => { const p = String(v || '').trim().split(/\s+/)[0].toLowerCase(); if (p.length >= 3 && /^[a-zà-ú]+$/.test(p)) contagem[p] = (contagem[p] || 0) + 1; };
      for (const r of db.prepare("SELECT nome, pet_nome FROM contatos").all()) { conta(r.nome); conta(r.pet_nome); }
      const temNome = t => (t.match(/[A-ZÀ-Ú][a-zà-ú]{2,}/g) || []).some(w => { const k = w.toLowerCase(); return !comuns.has(k) && (contagem[k] || 0) >= 2; });
      const rows = db.prepare(`SELECT texto as t, COUNT(*) as n, COUNT(DISTINCT contato_id) as c FROM mensagens
        WHERE direcao = 'out' AND tipo = 'text' AND length(texto) BETWEEN 12 AND 220 AND created_at >= datetime('now','localtime','-60 days')
        GROUP BY texto HAVING n >= 5 AND c >= 4 ORDER BY n DESC LIMIT 800`).all();
      frasesCache = { em: agora, lista: rows
        .filter(r => !/\d/.test(r.t) && !/R\$/.test(r.t) && !r.t.includes(followup.ATHENA_MARCA) && !/agradece seu contato/i.test(r.t) && !temNome(r.t))
        .slice(0, 250).map(r => ({ t: r.t.replace(/\u200e|\ufffd ?/g, '').trim(), n: r.n })) };
    }
    return sendJSON(res, 200, frasesCache.lista);
  }

  if (method === 'GET' && pathname === '/api/msgs-rapidas') {
    return sendJSON(res, 200, db.prepare('SELECT * FROM msgs_rapidas ORDER BY ordem, id').all());
  }
  if (method === 'POST' && pathname === '/api/msg-rapida') {
    const body = await parseBody(req);
    if (!body.titulo || !body.texto) return sendJSON(res, 400, { error: 'titulo e texto required' });
    const maxOrdem = db.prepare('SELECT COALESCE(MAX(ordem),0) as m FROM msgs_rapidas').get().m;
    const r = db.prepare('INSERT INTO msgs_rapidas (titulo, texto, ordem) VALUES (?, ?, ?)').run(body.titulo.trim(), body.texto.trim(), maxOrdem + 1);
    return sendJSON(res, 201, { ok: true, id: r.lastInsertRowid });
  }
  if (method === 'PUT' && pathname.match(/^\/api\/msg-rapida\/\d+$/)) {
    const id = pathname.split('/').pop();
    const body = await parseBody(req);
    const fields = [], values = [];
    if (body.titulo !== undefined) { fields.push('titulo = ?'); values.push(body.titulo.trim()); }
    if (body.texto !== undefined) { fields.push('texto = ?'); values.push(body.texto.trim()); }
    if (fields.length === 0) return sendJSON(res, 400, { error: 'No fields' });
    values.push(id);
    db.prepare(`UPDATE msgs_rapidas SET ${fields.join(', ')} WHERE id = ?`).run(...values);
    return sendJSON(res, 200, { ok: true });
  }
  if (method === 'DELETE' && pathname.match(/^\/api\/msg-rapida\/\d+$/)) {
    const id = pathname.split('/').pop();
    db.prepare('DELETE FROM msgs_rapidas WHERE id = ?').run(id);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Forward message ────────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/forward') {
    const body = await parseBody(req);
    // Várias de uma vez: vão na ordem em que foram escritas na conversa; arquivo sai com o nome original e a legenda.
    if (Array.isArray(body.msgIds)) {
      const ids = body.msgIds.map(Number).filter(n => n > 0).slice(0, 30);
      if (!ids.length) return sendJSON(res, 400, { error: 'Nenhuma mensagem selecionada.' });
      let alvo = '', paraContato = null, paraGrupo = null;
      if (body.toContatoId) {
        paraContato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(body.toContatoId);
        if (!paraContato) return sendJSON(res, 404, { error: 'Contato não encontrado' });
        alvo = paraContato.telefone;
      } else if (body.toGrupoId) {
        paraGrupo = db.prepare('SELECT * FROM grupos WHERE id = ?').get(body.toGrupoId);
        if (!paraGrupo) return sendJSON(res, 404, { error: 'Grupo não encontrado' });
        alvo = paraGrupo.jid;
      } else return sendJSON(res, 400, { error: 'Escolha para quem encaminhar.' });
      const msgs = db.prepare('SELECT * FROM mensagens WHERE id IN (' + ids.map(() => '?').join(',') + ') ORDER BY created_at, id').all(...ids);
      let enviadas = 0, puladas = 0;
      for (const msg of msgs) {
        try {
          let r = null;
          const texto = String(msg.texto || '');
          const arq = (msg.media_path && msg.tipo !== 'text') ? path.join(MEDIA_DIR, msg.media_path) : '';
          if (arq) {
            if (!fs.existsSync(arq)) { puladas++; continue; } // o arquivo não está mais guardado no servidor
            const ext = path.extname(msg.media_path).toLowerCase();
            const mediatype = ['.jpg', '.jpeg', '.png', '.webp'].includes(ext) ? 'image' : ext === '.mp4' ? 'video' : ['.ogg', '.mp3', '.opus'].includes(ext) ? 'audio' : 'document';
            let nome = msg.media_path, legenda = texto.startsWith('[') ? '' : texto;
            if (mediatype === 'document') {
              // documento é gravado como "nome do arquivo" + (linha de baixo) legenda
              const linhas = legenda.split('\n');
              if (linhas[0] && /\.[a-z0-9]{2,5}$/i.test(linhas[0].trim())) { nome = linhas[0].trim(); legenda = linhas.slice(1).join('\n'); }
            }
            r = await evoSendMedia(alvo, mediatype, fs.readFileSync(arq).toString('base64'), mediatype === 'audio' ? '' : legenda, nome);
          } else if (msg.tipo === 'contact' && msg.cartao) {
            let cs = [];
            try { cs = JSON.parse(msg.cartao) || []; } catch {}
            if (!cs.length) { puladas++; continue; }
            r = await enviarCartao(alvo, cs);
          } else {
            if (!texto || msg.tipo !== 'text') { puladas++; continue; }
            r = await evoSendText(alvo, texto);
          }
          const chave = (r && r.key) || {};
          if (paraContato) {
            db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path, wa_msg_id, wa_remote_jid, cartao) VALUES (?, 'out', ?, ?, ?, ?, ?, ?)").run(
              paraContato.id, texto, msg.tipo, msg.media_path || '', chave.id || '', chave.remoteJid || jidFromPhone(paraContato.telefone), msg.cartao || '');
          } else {
            db.prepare("INSERT INTO grupo_mensagens (grupo_id, remetente, remetente_nome, texto, tipo) VALUES (?, 'me', 'Eu', ?, ?)").run(paraGrupo.id, texto || '[mídia encaminhada]', msg.tipo);
          }
          enviadas++;
          await new Promise(ok => setTimeout(ok, 350)); // respiro para chegarem na ordem
        } catch (e) {
          console.error('[CRM] encaminhar várias: parou na mensagem ' + msg.id + ':', e.message);
          if (paraContato && enviadas) db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(paraContato.id);
          return sendJSON(res, 500, { error: (enviadas ? 'Foram ' + enviadas + ' de ' + msgs.length + '; parou porque ' : 'Falha ao encaminhar: ') + e.message, enviadas });
        }
      }
      if (paraContato && enviadas) db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(paraContato.id);
      console.log('[CRM] encaminhadas ' + enviadas + ' mensagens' + (puladas ? ' (' + puladas + ' sem arquivo, puladas)' : '') + ' para ' + (paraContato ? 'contato ' + paraContato.id : 'grupo ' + paraGrupo.id));
      return sendJSON(res, 200, { ok: true, enviadas, puladas });
    }

    const { msgId, toContatoId, toGrupoId } = body;
    if (!msgId) return sendJSON(res, 400, { error: 'msgId required' });
    if (!toContatoId && !toGrupoId) return sendJSON(res, 400, { error: 'toContatoId or toGrupoId required' });

    // Find the original message
    const msg = db.prepare('SELECT * FROM mensagens WHERE id = ?').get(msgId);
    if (!msg) return sendJSON(res, 404, { error: 'Message not found' });

    // Determine destination phone/jid
    let destPhone = null;
    let destGrupo = null;
    if (toContatoId) {
      const dest = db.prepare('SELECT * FROM contatos WHERE id = ?').get(toContatoId);
      if (!dest) return sendJSON(res, 404, { error: 'Contato not found' });
      destPhone = dest.telefone;
    } else {
      destGrupo = db.prepare('SELECT * FROM grupos WHERE id = ?').get(toGrupoId);
      if (!destGrupo) return sendJSON(res, 404, { error: 'Grupo not found' });
    }

    const target = destPhone || destGrupo.jid;

    try {
      if (msg.media_path && msg.tipo !== 'text') {
        // Forward media
        const filePath = path.join(MEDIA_DIR, msg.media_path);
        if (fs.existsSync(filePath)) {
          const base64 = fs.readFileSync(filePath).toString('base64');
          const ext = path.extname(msg.media_path).toLowerCase();
          const mediatype = ['.jpg', '.jpeg', '.png', '.webp'].includes(ext) ? 'image'
            : ['.mp4'].includes(ext) ? 'video'
            : ['.ogg', '.mp3', '.opus'].includes(ext) ? 'audio'
            : 'document';
          const caption = (msg.texto && !msg.texto.startsWith('[')) ? msg.texto : '';
          await evoSendMedia(target, mediatype, base64, caption, msg.media_path);
        } else {
          // File missing, send text fallback
          await evoSendText(target, msg.texto || '[mídia não disponível]');
        }
      } else {
        // Forward text
        await evoSendText(target, msg.texto);
      }

      // Save forwarded message in destination
      if (toContatoId) {
        db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path) VALUES (?, 'out', ?, ?, ?)").run(
          toContatoId, msg.texto || '', msg.tipo, msg.media_path || ''
        );
        db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(toContatoId);
      } else if (toGrupoId) {
        db.prepare("INSERT INTO grupo_mensagens (grupo_id, remetente, remetente_nome, texto, tipo) VALUES (?, 'me', 'Eu', ?, ?)").run(
          toGrupoId, msg.texto || '[mídia encaminhada]', msg.tipo
        );
      }

      console.log('[CRM] Forwarded msg ' + msgId + ' to ' + target);
      return sendJSON(res, 200, { ok: true });
    } catch (e) {
      console.error('[CRM] Forward error:', e.message);
      return sendJSON(res, 500, { error: 'Falha ao encaminhar: ' + e.message });
    }
  }

  // ─── API: Métricas ─────────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/metricas') {
    const periodo = urlObj.searchParams.get('periodo') || '30'; // dias
    const dias = parseInt(periodo) || 30;
    const desde = `datetime('now','localtime','-${dias} days')`;

    // 1. Funil: quantos contatos passaram por cada etapa no período
    const etapasAll = db.prepare('SELECT id, label, ordem, cor FROM etapas_config ORDER BY ordem').all();
    const funil = etapasAll.map(e => {
      const passaram = db.prepare(`
        SELECT COUNT(DISTINCT contato_id) as total FROM etapa_historico
        WHERE etapa_para = ? AND created_at >= ${desde}
      `).get(e.id);
      // Quantos ainda estão nessa etapa
      const atuais = db.prepare('SELECT COUNT(*) as total FROM contatos WHERE etapa = ?').get(e.id);
      return { id: e.id, label: e.label, cor: e.cor, passaram: passaram.total, atuais: atuais.total };
    });

    // 2. Tempo médio por etapa (em horas)
    // Calcula quanto tempo cada contato ficou em cada etapa usando histórico de transições
    const tempoMedio = etapasAll.map(e => {
      const rows = db.prepare(`
        SELECT h1.contato_id, h1.created_at as entrou,
          (SELECT MIN(h2.created_at) FROM etapa_historico h2
           WHERE h2.contato_id = h1.contato_id AND h2.etapa_de = h1.etapa_para AND h2.created_at > h1.created_at
          ) as saiu
        FROM etapa_historico h1
        WHERE h1.etapa_para = ? AND h1.created_at >= ${desde}
      `).all(e.id);

      let totalHoras = 0, count = 0;
      for (const r of rows) {
        if (r.saiu) {
          const diff = (new Date(r.saiu) - new Date(r.entrou)) / 3600000;
          if (diff > 0) { totalHoras += diff; count++; }
        }
      }
      return { id: e.id, label: e.label, cor: e.cor, media_horas: count > 0 ? Math.round(totalHoras / count * 10) / 10 : null, amostras: count };
    });

    // 3. Novos contatos por dia (últimos N dias)
    const novosPorDia = db.prepare(`
      SELECT date(created_at) as dia, COUNT(*) as total
      FROM contatos WHERE created_at >= ${desde}
      GROUP BY date(created_at) ORDER BY dia
    `).all();

    // 4. Taxa de conversão: quantos chegaram a finalizado vs total de novos
    const totalNovos = db.prepare(`SELECT COUNT(DISTINCT contato_id) as c FROM etapa_historico WHERE etapa_para = 'novo' AND created_at >= ${desde}`).get().c;
    const totalFinalizados = db.prepare(`SELECT COUNT(DISTINCT contato_id) as c FROM etapa_historico WHERE etapa_para = 'finalizado' AND created_at >= ${desde}`).get().c;
    const taxaConversao = totalNovos > 0 ? Math.round(totalFinalizados / totalNovos * 1000) / 10 : 0;

    // 5. Contatos parados (sem mensagem há 3+ dias, não finalizados)
    const parados = db.prepare(`
      SELECT c.id, c.nome, c.telefone, c.etapa, c.last_message_at,
        ROUND((julianday('now','localtime') - julianday(c.last_message_at)) * 24) as horas_parado
      FROM contatos c
      WHERE c.etapa != 'finalizado'
      AND c.last_message_at < datetime('now','localtime','-3 days')
      ORDER BY c.last_message_at ASC LIMIT 20
    `).all();

    return sendJSON(res, 200, { funil, tempoMedio, novosPorDia, taxaConversao, totalNovos, totalFinalizados, parados, periodo: dias });
  }

  // ─── API: Métricas de vendas (visão do gestor) ─────────────────────────────
  // (/api/metricas sem sufixo já existe mais acima — é a tela antiga de etapas)
  // Dois caminhos pro mesmo dado: bloqueadores de anúncio costumam barrar URLs com "metric".
  if (method === 'GET' && (pathname === '/api/metricas/vendas' || pathname === '/api/painel/vendas')) {
    const dias = Number(urlObj.searchParams.get('dias')) || 30;
    const t0 = Date.now();
    console.log(`[METRICAS] pedido dias=${dias} de ${req.headers['x-forwarded-for'] || req.socket.remoteAddress} ua=${String(req.headers['user-agent'] || '').slice(0, 40)}`);
    try {
      const out = metricas.calcular(db, dias);
      // Por origem (anúncio Meta / Google / direto): quem chegou no período e quanto virou exame.
      try { out.origens = origem.resumo(db, dias, followup.ATHENA_MARCA); } catch (e) { console.error('[ORIGEM] resumo:', e.message); }
      // Modo sombra: placar do agente que redige em silêncio (nada é enviado a clientes).
      try { out.sombra = sombra.resumo(db, dias); } catch (e) { console.error('[SOMBRA] resumo:', e.message); }
      try { out.leitor = leitor.resumo(db, dias); } catch (e) { console.error('[LEITOR] resumo:', e.message); }
      console.log(`[METRICAS] ok em ${Date.now() - t0}ms`);
      return sendJSON(res, 200, out);
    } catch (e) { console.error('[METRICAS] erro:', e.message); return sendJSON(res, 500, { error: e.message }); }
  }

  // ─── API: Origem dos contatos (anúncio Meta / Google / direto) — usado pelo relatório da Dani ──
  if (method === 'GET' && pathname === '/api/painel/origem') {
    const dias = Number(urlObj.searchParams.get('dias')) || 30;
    try { return sendJSON(res, 200, origem.resumo(db, dias, followup.ATHENA_MARCA)); }
    catch (e) { console.error('[ORIGEM] erro:', e.message); return sendJSON(res, 500, { error: e.message }); }
  }

  // ─── API: Modo sombra — placar do agente em teste (somente leitura) ────────
  if (method === 'GET' && pathname === '/api/painel/sombra') {
    const dias = Number(urlObj.searchParams.get('dias')) || 30;
    try { return sendJSON(res, 200, sombra.resumo(db, dias)); }
    catch (e) { console.error('[SOMBRA] erro:', e.message); return sendJSON(res, 500, { error: e.message }); }
  }

  // ─── API: Follow-up — ações pela tela (mesmos comandos do WhatsApp) ────────
  if (method === 'POST' && pathname.match(/^\/api\/followup\/\d+\/(marcou|desistiu|fechou|perdeu|ok|ignorar)$/)) {
    const parts = pathname.split('/');
    const id = Number(parts[3]);
    const cmd = parts[4];
    const msg = followup.executarComando(db, cmd, id);
    return sendJSON(res, 200, { ok: true, msg: msg || '' });
  }

  // ─── API: Follow-up (fila de orçamentos a cobrar) ─────────────────────────
  if (method === 'GET' && pathname === '/api/followup') {
    const fila = db.prepare(`
      SELECT c.id, c.nome, c.telefone, c.pet_nome, c.foto_url, c.etapa,
        c.followup_nivel, c.followup_desde, c.followup_motivo, c.orcamento_enviado_at,
        c.orcamento_modalidade, COALESCE(NULLIF(c.origem,''),'direto') AS origem,
        (SELECT texto FROM mensagens WHERE contato_id = c.id ORDER BY id DESC LIMIT 1) as ultima_msg,
        (SELECT direcao FROM mensagens WHERE contato_id = c.id ORDER BY id DESC LIMIT 1) as ultima_msg_dir,
        (SELECT created_at FROM mensagens WHERE contato_id = c.id ORDER BY id DESC LIMIT 1) as ultima_msg_at
      FROM contatos c
      WHERE c.followup_nivel > 0
      ORDER BY c.followup_nivel ASC, c.followup_desde ASC
    `).all();
    const resumo = { urgente: 0, followup: 0, segunda: 0 };
    for (const f of fila) { if (f.followup_nivel === 1) resumo.urgente++; else if (f.followup_nivel === 2) resumo.followup++; else resumo.segunda++; }
    // Orçamento parado (🟡/⚪): já vai com a mensagem de retomada sugerida (1ª, 2ª ou última).
    const agoraFu = new Date();
    for (const f of fila) {
      try {
        // 🔴 parado de um dia para o outro: sugestão de retomada com pedido de desculpas (recente não tem: é responder a pergunta).
        const s = f.followup_nivel < 2 ? retomada.sugerirVermelho(db, f, agoraFu) : retomada.sugerir(db, f, agoraFu);
        if (s) f.retomada = { etapa: s.etapa, feitas: s.feitas, rotulo: s.rotulo, motivo: s.motivo, texto: s.texto, preencher: s.preencher };
      } catch (e) { console.error('[RETOMADA] sugerir:', e.message); }
    }
    // Últimas mensagens de cada conversa, para a recepção conferir se a sugestão combina com o que o tutor escreveu.
    const qUlt = db.prepare("SELECT id, direcao, texto, tipo, media_path, created_at FROM mensagens WHERE contato_id = ? ORDER BY id DESC LIMIT 5");
    const qUltIn = db.prepare("SELECT id, direcao, texto, tipo, media_path, created_at FROM mensagens WHERE contato_id = ? AND direcao = 'in' ORDER BY id DESC LIMIT 1");
    for (const f of fila) {
      try {
        const ult = qUlt.all(f.id);
        if (!ult.some(m => m.direcao === 'in')) { const ui = qUltIn.get(f.id); if (ui) ult.push(ui); }  // sempre aparece a última fala do tutor
        leitor.anexarLeituras(db, ult);
        f.conversa = ult.reverse().map(m => ({ de: m.direcao, quando: m.created_at, tipo: m.tipo, texto: textoParaCard(m) }));
      } catch (e) { console.error('[FOLLOWUP] conversa:', e.message); }
    }
    return sendJSON(res, 200, { resumo, regras: followup.REGRAS, fila });
  }

  // ─── API: Stats ───────────────────────────────────────────────────────────
  if (method === 'GET' && pathname === '/api/stats') {
    const stats = db.prepare(`
      SELECT etapa, COUNT(*) as total FROM contatos GROUP BY etapa
    `).all();
    const parados = db.prepare(`
      SELECT COUNT(*) as total FROM contatos
      WHERE etapa NOT IN ('finalizado')
      AND last_message_at < datetime('now', '-3 days', 'localtime')
    `).get();
    return sendJSON(res, 200, { por_etapa: stats, parados_3dias: parados.total });
  }

  return sendJSON(res, 404, { error: 'Not found' });
}

// ─── HTTP Server ───────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  try {
    await handleRequest(req, res);
  } catch (e) {
    console.error('[CRM] Error:', e);
    sendJSON(res, 500, { error: e.message });
  }
});

server.listen(PORT, () => {
  console.log(`[XR-CRM] Running on port ${PORT}`);

  // ─── Follow-up de orçamentos ─────────────────────────────────────────────────
  // Uma vez: devolve para Agendado quem caiu em Orçamento por causa do PDF do laudo.
  try { followup.corrigirLaudos(db); } catch (e) { console.error('[CRM] corrigirLaudos:', e.message); }
  // Uma vez: origem (anúncio/Google/direto) de quem já estava no banco, pelas primeiras mensagens.
  try { origem.backfill(db); } catch (e) { console.error('[ORIGEM] backfill:', e.message); }
  // Modo sombra: o agente redige em silêncio e é comparado com a recepção. Não envia nada. (SOMBRA=0 desliga)
  try { sombra.iniciar(db); } catch (e) { console.error('[SOMBRA] iniciar:', e.message); }
  // Leitor de anexos: transcreve o pedido do veterinário enviado em foto ou PDF. Não envia nada. (LEITOR=0 desliga)
  try { leitor.iniciar(db); } catch (e) { console.error('[LEITOR] iniciar:', e.message); }
  // A cada 5 min: encerra desistências/luto e recalcula os níveis 🔴🟡⚪.
  followup.iniciarVigia(db, {
    intervaloMs: 5 * 60 * 1000,
    // 🔴 novos avisam a recepção na hora (só em horário útil). 🟡/⚪ vão no digest.
    aoMudar: subiram => alerta.notificarNovos(db, subiram, followup.emHorarioUtil),
  });
  // Digest 08:05 (sempre — é o heartbeat) e 13:05 (só se tiver pendência).
  alerta.iniciarDigest(db, followup.emHorarioUtil);
  console.log(alerta.LIGADO ? `[ALERTA] canal ativo → recepção ${alerta.DESTINO} via ${alerta.INSTANCIA} (${alerta.ORIGEM})`
    : '[ALERTA] avisos no WhatsApp da recepção DESLIGADOS (ALERTA_RECEPCAO=1 religa). A aba Follow-up do CRM continua igual.');

  // ─── Auto-finalize: contacts in "agendado" for 10+ days (once per hour) ────
  setInterval(() => {
    try {
      const stale = db.prepare(`
        SELECT id, nome, telefone, etapa FROM contatos
        WHERE etapa = 'agendado'
        AND updated_at < datetime('now', '-10 days', 'localtime')
      `).all();
      for (const c of stale) {
        db.prepare("UPDATE contatos SET etapa = 'finalizado', updated_at = datetime('now','localtime') WHERE id = ?").run(c.id);
        db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'agendado', 'finalizado')").run(c.id);
        console.log(`[CRM] Auto-finalizado: ${c.nome || c.telefone} (10 dias em Agendado)`);
      }
    } catch {}
  }, 3600000); // Once per hour
});
