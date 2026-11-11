'use strict';

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const followup = require('./followup');
const alerta = require('./alerta');
const metricas = require('./metricas');
const origem = require('./origem');
const retomada = require('./retomada');

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
try { db.exec("ALTER TABLE contatos ADD COLUMN pinado INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN favorito INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN orcamento_enviado_at TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE contatos ADD COLUMN ultima_resposta_cliente_at TEXT DEFAULT ''"); } catch {}
followup.initFollowup(db);
origem.init(db);

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
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' });
  res.end(body);
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
function evoSendText(phone, text) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${EVO_BASE}/message/sendText/${EVO_INSTANCE}`);
    const number = phone.includes('@g.us') ? phone : phone.replace(/\D/g, '');
    const payload = JSON.stringify({ number, text });
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname,
      method: 'POST',
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

// Mark messages as read on WhatsApp (sync CRM → WhatsApp)
function evoMarkAsRead(phone) {
  const jid = phone.includes('@') ? phone : `${phone}@s.whatsapp.net`;
  const payload = JSON.stringify({ readMessages: [{ remoteJid: jid, fromMe: false, id: 'all' }] });
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

function evoSendMedia(phone, mediatype, base64, caption, fileName) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${EVO_BASE}/message/sendMedia/${EVO_INSTANCE}`);
    const payload = JSON.stringify({
      number: phone.replace(/\D/g, ''),
      mediatype,
      media: base64,
      caption: caption || '',
      fileName: fileName || ''
    });
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'apikey': EVO_KEY, 'Content-Length': Buffer.byteLength(payload) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve({ raw: d }); } });
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

// ─── Routes ────────────────────────────────────────────────────────────────────
async function handleRequest(req, res) {
  const urlObj = new URL(req.url, 'http://localhost');
  const pathname = urlObj.pathname;
  const method = req.method;

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
  if (method === 'GET' && pathname.match(/^\/api\/contato\/\d+$/)) {
    const id = pathname.split('/').pop();
    const contato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Not found' });
    contato.mensagens = db.prepare('SELECT * FROM mensagens WHERE contato_id = ? ORDER BY created_at DESC LIMIT 50').all(id);
    contato.etiquetas = db.prepare('SELECT e.* FROM etiquetas e JOIN contato_etiquetas ce ON ce.etiqueta_id = e.id WHERE ce.contato_id = ?').all(id);
    // Mark as read + reset manual_unread
    db.prepare("UPDATE contatos SET last_read_at = datetime('now','localtime'), manual_unread = 0 WHERE id = ?").run(id);
    // Sync: mark as read on real WhatsApp too
    if (contato.telefone) evoMarkAsRead(contato.telefone);
    return sendJSON(res, 200, contato);
  }

  // ─── API: Create contato ──────────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/contato') {
    const body = await parseBody(req);
    const { nome, telefone, pet_nome, pet_especie, veterinario, etapa, notas } = body;
    if (!telefone) return sendJSON(res, 400, { error: 'telefone required' });
    const phone = telefone.replace(/\D/g, '');
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
    if (old) db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, ?)").run(id, old.etapa, body.etapa);
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
    const rows = db.prepare(`
      SELECT c.*,
        (SELECT texto FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg,
        (SELECT COUNT(*) FROM mensagens WHERE contato_id = c.id AND direcao = 'in' AND created_at > COALESCE(NULLIF(c.last_read_at,''), '1970-01-01')) as nao_lidas,
        (SELECT direcao FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_dir,
        (SELECT created_at FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_at,
        (SELECT tipo FROM mensagens WHERE contato_id = c.id ORDER BY created_at DESC LIMIT 1) as ultima_msg_tipo
      FROM contatos c
      WHERE NOT (c.etapa = 'finalizado' AND c.updated_at < datetime('now','localtime','-2 days'))
      ORDER BY c.pinado DESC, c.last_message_at DESC
    `).all();
    const getEtiquetas = db.prepare('SELECT e.* FROM etiquetas e JOIN contato_etiquetas ce ON ce.etiqueta_id = e.id WHERE ce.contato_id = ?');
    for (const row of rows) { row.etiquetas = getEtiquetas.all(row.id); }
    return sendJSON(res, 200, rows);
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
    const contato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Contato not found' });

    // Send via Evolution
    let waKey = null;
    try {
      const result = await evoSendText(contato.telefone, body.texto);
      if (result && result.key) waKey = result.key;
    } catch (e) {
      console.error('[CRM] Erro enviando msg:', e.message);
      return sendJSON(res, 500, { error: 'Falha ao enviar: ' + e.message });
    }

    // Save message
    const waMsgId = (waKey && waKey.id) || '';
    const waJid = (waKey && waKey.remoteJid) || jidFromPhone(contato.telefone);
    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, wa_msg_id, wa_remote_jid) VALUES (?, 'out', ?, 'text', ?, ?)").run(id, body.texto, waMsgId, waJid);
    db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(id);
    // Mensagem enviada pela tela do CRM a um orçamento parado também conta como retomada.
    try { retomada.registrarSeRetomada(db, Number(id), body.texto, 'text'); } catch (e) { console.error('[RETOMADA]', e.message); }
    return sendJSON(res, 200, { ok: true });
  }

  // ─── API: Send media via Evolution ─────────────────────────────────────────
  if (method === 'POST' && pathname.match(/^\/api\/contato\/\d+\/media$/)) {
    const id = pathname.split('/')[3];
    const body = await parseBody(req, 50e6);
    if (!body.base64 || !body.mediatype) return sendJSON(res, 400, { error: 'base64 and mediatype required' });
    const contato = db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
    if (!contato) return sendJSON(res, 404, { error: 'Contato not found' });

    // Send via Evolution
    try {
      await evoSendMedia(contato.telefone, body.mediatype, body.base64, body.caption || '', body.fileName || '');
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

    const msgTexto = body.caption || body.fileName || `[${body.mediatype}]`;
    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path) VALUES (?, 'out', ?, ?, ?)").run(
      id, msgTexto, body.mediatype === 'image' ? 'image' : 'document', filename
    );
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
    const texto = msg.conversation || (msg.extendedTextMessage && msg.extendedTextMessage.text) || '';
    console.log(`[CRM] Msg ${isFromMe ? 'OUT' : 'IN'} ${phone}: texto="${texto.substring(0,50)}" tipo=${Object.keys(msg).join(',')}`);
    const tipo = msg.audioMessage ? 'audio' : msg.imageMessage ? 'image' : msg.documentMessage ? 'document' : 'text';

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
        `).run('', phone);
        contato = { id: result.lastInsertRowid, etapa: 'novo' };
        db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, '', 'novo')").run(contato.id);
        console.log(`[CRM] Novo contato (fromMe): ${phone}`);
      } else {
        const fotoUrl = await evoFetchProfilePic(phone);
        const result = db.prepare(`
          INSERT INTO contatos (nome, telefone, etapa, foto_url, last_message_at, updated_at)
          VALUES (?, ?, 'novo', ?, datetime('now','localtime'), datetime('now','localtime'))
        `).run(pushName, phone, fotoUrl);
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
      if (tipo !== 'text') {
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
      db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path, wa_msg_id, wa_remote_jid) VALUES (?, ?, ?, ?, ?, ?, ?)").run(
        contato.id, direcao, texto || caption || docName || docCaption || `[${tipo}]`, tipo, mediaPath, waMsgId, waJid
      );
      // De onde o cliente veio (anúncio Meta, Google, direto) — só leitura das primeiras mensagens dele.
      if (!isFromMe) { try { origem.classificar(db, contato.id, texto || caption); } catch (e) { console.error('[ORIGEM]', e.message); } }
      // A recepção escreveu para um orçamento parado (🟡/⚪)? Conta como retomada (1ª, 2ª ou última).
      else { try { retomada.registrarSeRetomada(db, contato.id, texto || caption || docName || docCaption, tipo); } catch (e) { console.error('[RETOMADA]', e.message); } }
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
      if (f.followup_nivel < 2) continue;
      try { const s = retomada.sugerir(db, f, agoraFu); f.retomada = { etapa: s.etapa, feitas: s.feitas, rotulo: s.rotulo, motivo: s.motivo, texto: s.texto, preencher: s.preencher }; }
      catch (e) { console.error('[RETOMADA] sugerir:', e.message); }
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
  // A cada 5 min: encerra desistências/luto e recalcula os níveis 🔴🟡⚪.
  followup.iniciarVigia(db, {
    intervaloMs: 5 * 60 * 1000,
    // 🔴 novos avisam a recepção na hora (só em horário útil). 🟡/⚪ vão no digest.
    aoMudar: subiram => alerta.notificarNovos(db, subiram, followup.emHorarioUtil),
  });
  // Digest 08:05 (sempre — é o heartbeat) e 13:05 (só se tiver pendência).
  alerta.iniciarDigest(db, followup.emHorarioUtil);
  console.log(`[ALERTA] canal ativo → recepção ${alerta.DESTINO} via ${process.env.ALERTA_INSTANCE || 'xr-prevenvet'}`);

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
