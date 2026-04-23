'use strict';

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

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
try { db.exec("ALTER TABLE mensagens ADD COLUMN wa_remote_jid TEXT DEFAULT ''"); } catch {}

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
    ['aguardando_exame', 'Aguardando Exame', 3, '#8b5cf6'],
    ['agendamento', 'Agendamento', 4, '#3b82f6'],
    ['confirmado', 'Confirmado', 5, '#10b981'],
    ['resultado', 'Resultado', 6, '#f97316'],
    ['finalizado', 'Finalizado', 7, '#6b7280'],
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
    // Mark as read
    db.prepare("UPDATE contatos SET last_read_at = datetime('now','localtime') WHERE id = ?").run(id);
    return sendJSON(res, 200, contato);
  }

  // ─── API: Create contato ──────────────────────────────────────────────────
  if (method === 'POST' && pathname === '/api/contato') {
    const body = await parseBody(req);
    const { nome, telefone, pet_nome, pet_especie, veterinario, etapa, notas } = body;
    if (!telefone) return sendJSON(res, 400, { error: 'telefone required' });
    const phone = telefone.replace(/\D/g, '');
    try {
      const result = db.prepare(`
        INSERT INTO contatos (nome, telefone, pet_nome, pet_especie, veterinario, etapa, notas)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(nome || '', phone, pet_nome || '', pet_especie || '', veterinario || '', etapa || 'novo', notas || '');
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
    db.prepare("UPDATE contatos SET etapa = ?, updated_at = datetime('now','localtime') WHERE id = ?").run(body.etapa, id);
    return sendJSON(res, 200, { ok: true });
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
    const mime = body.mimetype || '';
    const ext = body.fileName ? path.extname(body.fileName) : (mime.includes('jpeg') || mime.includes('jpg') ? '.jpg' : mime.includes('png') ? '.png' : mime.includes('pdf') ? '.pdf' : '');
    const filename = `${Date.now()}_${contato.id}_out${ext}`;
    fs.writeFileSync(path.join(MEDIA_DIR, filename), Buffer.from(body.base64, 'base64'));

    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path) VALUES (?, 'out', ?, ?, ?)").run(
      id, body.caption || `[${body.mediatype}]`, body.mediatype === 'image' ? 'image' : 'document', filename
    );
    db.prepare("UPDATE contatos SET updated_at = datetime('now','localtime'), last_message_at = datetime('now','localtime') WHERE id = ?").run(id);
    return sendJSON(res, 200, { ok: true });
  }

  // ─── Webhook: Evolution API ───────────────────────────────────────────────
  if (method === 'POST' && pathname === '/webhook/evolution') {
    const body = await parseBody(req);
    console.log('[CRM] Webhook:', JSON.stringify(body).substring(0, 300));

    const event = body.event;
    if (event !== 'messages.upsert' && event !== 'MESSAGES_UPSERT') {
      return sendJSON(res, 200, { ok: true, ignored: true });
    }

    const data = body.data || body;
    const key = data.key || {};
    const jid = key.remoteJid || '';

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

    // Skip status updates (no actual message content)
    const status = data.status || '';
    if (!data.message && (status === 'SERVER_ACK' || status === 'DELIVERY_ACK' || status === 'READ' || status === 'PLAYED')) {
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
        console.log(`[CRM] Novo contato (fromMe): ${phone}`);
      } else {
        const fotoUrl = await evoFetchProfilePic(phone);
        const result = db.prepare(`
          INSERT INTO contatos (nome, telefone, etapa, foto_url, last_message_at, updated_at)
          VALUES (?, ?, 'novo', ?, datetime('now','localtime'), datetime('now','localtime'))
        `).run(pushName, phone, fotoUrl);
        contato = { id: result.lastInsertRowid, etapa: 'novo' };
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
        console.log(`[CRM] Contato ${contato.nome || phone} voltou de finalizado para novo`);
      } else {
        db.prepare("UPDATE contatos SET last_message_at = datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE id = ?").run(contato.id);
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
      db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path, wa_msg_id, wa_remote_jid) VALUES (?, ?, ?, ?, ?, ?, ?)").run(
        contato.id, direcao, texto || caption || `[${tipo}]`, tipo, mediaPath, waMsgId, waJid
      );
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
    const rows = db.prepare(`
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

server.listen(PORT, () => console.log(`[XR-CRM] Running on port ${PORT}`));
