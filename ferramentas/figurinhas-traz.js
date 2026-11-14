// Passo 2 (dentro do container, na pasta do xr-crm): põe no CRM as figurinhas listadas em _figurinhas.json.
//   node ferramentas/figurinhas-traz.js            → só mostra
//   node ferramentas/figurinhas-traz.js aplicar    → grava a mensagem (na hora em que foi enviada) e baixa o arquivo
// Só entra figurinha de conversa que já existe no CRM (contato ou grupo acompanhado) e que ainda não está lá.
const fs = require('fs');
const path = require('path');
const http = require('http');
const { execSync } = require('child_process');
const Database = require('better-sqlite3');
const APLICAR = process.argv[2] === 'aplicar';
const pid = execSync('pm2 pid xr-crm').toString().trim();
const env = {};
fs.readFileSync('/proc/' + pid + '/environ', 'utf8').split('\0').forEach(l => { const i = l.indexOf('='); if (i > 0 && /^EVO_/.test(l)) env[l.slice(0, i)] = l.slice(i + 1); });
const BASE = env.EVO_BASE || 'http://172.18.0.1:8085';
const INST = env.EVO_INSTANCE || 'xr-whatsapp';
function post(caminho, corpo) {
  return new Promise((resolve, reject) => {
    const u = new URL(BASE + caminho);
    const p = JSON.stringify(corpo || {});
    const r = http.request({ hostname: u.hostname, port: u.port, path: u.pathname, method: 'POST', headers: { 'Content-Type': 'application/json', apikey: env.EVO_KEY, 'Content-Length': Buffer.byteLength(p) } }, res => {
      let d = ''; res.on('data', c => d += c); res.on('end', () => { let j = null; try { j = JSON.parse(d); } catch (e) {} resolve({ status: res.statusCode, json: j }); });
    });
    r.on('error', reject); r.setTimeout(40000, () => { r.destroy(); reject(new Error('demorou')); });
    r.write(p); r.end();
  });
}
const quando = ts => { const d = new Date((Number(ts) - 3 * 3600) * 1000); return d.toISOString().slice(0, 19).replace('T', ' '); }; // horário de Brasília
const variantes = n => {
  const v = [n];
  if (n.startsWith('55') && n.length === 13 && n[4] === '9') v.push(n.slice(0, 4) + n.slice(5));
  if (n.startsWith('55') && n.length === 12 && /[6-9]/.test(n[4])) v.push(n.slice(0, 4) + '9' + n.slice(4));
  return v;
};

(async () => {
  const itens = JSON.parse(fs.readFileSync('_figurinhas.json', 'utf8'));
  const db = new Database('crm.db');
  db.pragma('busy_timeout = 8000');
  const contatos = new Map(db.prepare('SELECT id, telefone FROM contatos').all().map(c => [c.telefone, c.id]));
  const grupos = new Map(db.prepare('SELECT id, jid FROM grupos WHERE ativo = 1').all().map(g => [g.jid, g.id]));
  const cont = { total: itens.length, jaTem: 0, semConversa: 0, postas: 0, comArquivo: 0, semArquivo: 0 };
  for (const f of itens) {
    const jid = String(f.jid || ''), eu = String(f.eu) === 'true';
    const ehGrupo = jid.endsWith('@g.us');
    if (!f.id) continue;
    if (db.prepare('SELECT 1 FROM ' + (ehGrupo ? 'grupo_mensagens' : 'mensagens') + ' WHERE wa_msg_id = ? LIMIT 1').get(f.id)) { cont.jaTem++; continue; }
    let dono = null;
    if (ehGrupo) dono = grupos.get(jid);
    else {
      const tel = (jid.endsWith('@s.whatsapp.net') ? jid : String(f.alt || '')).split('@')[0];
      if (tel) dono = variantes(tel).map(v => contatos.get(v)).find(Boolean);
    }
    if (!dono) { cont.semConversa++; continue; }
    cont.postas++;
    if (!APLICAR) continue;
    const hora = quando(f.ts);
    let linha;
    if (ehGrupo) {
      linha = db.prepare("INSERT INTO grupo_mensagens (grupo_id, remetente, remetente_nome, texto, tipo, wa_msg_id, created_at) VALUES (?, ?, ?, '[sticker]', 'sticker', ?, ?)")
        .run(dono, eu ? 'me' : (f.part || f.nome || ''), eu ? 'Eu' : (f.nome || ''), f.id, hora).lastInsertRowid;
    } else {
      linha = db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, media_path, wa_msg_id, wa_remote_jid, created_at) VALUES (?, ?, '[sticker]', 'sticker', '', ?, ?, ?)")
        .run(dono, eu ? 'out' : 'in', f.id, jid, hora).lastInsertRowid;
    }
    try {
      const chave = { id: f.id, remoteJid: jid, fromMe: eu };
      if (ehGrupo && f.part) chave.participant = f.part;
      const r = await post('/chat/getBase64FromMediaMessage/' + INST, { message: { key: chave }, convertToMp4: false });
      const b64 = r.json && r.json.base64;
      if (r.status < 400 && b64) {
        const nome = Date.now() + '_' + (ehGrupo ? 'g' : '') + dono + '_fig.webp';
        fs.writeFileSync(path.join('media', nome), Buffer.from(b64, 'base64'));
        db.prepare('UPDATE ' + (ehGrupo ? 'grupo_mensagens' : 'mensagens') + ' SET media_path = ? WHERE id = ?').run(nome, linha);
        cont.comArquivo++;
      } else cont.semArquivo++;
    } catch (e) { cont.semArquivo++; }
    await new Promise(ok => setTimeout(ok, 300));
  }
  console.log('figurinhas na lista:', cont.total, '| já estavam no CRM:', cont.jaTem, '| de conversa que não existe no CRM:', cont.semConversa, '| a trazer:', cont.postas,
    APLICAR ? '| com o arquivo: ' + cont.comArquivo + ' | sem o arquivo (o WhatsApp não entregou mais): ' + cont.semArquivo : '(SÓ MOSTREI, nada gravado)');
})().catch(e => console.log('ERRO', e.message));
