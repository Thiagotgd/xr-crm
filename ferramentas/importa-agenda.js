// Importa a agenda do celular da recepção para o CRM. Roda dentro do container, na pasta do xr-crm.
//   node importa-agenda.js <agenda.json> [aplicar]        (sem "aplicar" só mostra o que faria; nomes não são impressos)
// Para cada telefone da agenda acha o número como o WhatsApp conhece (55 + DDD, com ou sem o nono dígito):
//   1º nos contatos do CRM, 2º nos contatos que a Evolution já conhece, 3º perguntando à Evolution (em lotes pequenos).
// Grava em "agenda" (telefone, nome). Contato que já existe no CRM passa a ter o nome da agenda; o nome anterior fica
// guardado em contatos.nome_antes_agenda. Nada é apagado.
const fs = require('fs');
const http = require('http');
const { execSync } = require('child_process');
const Database = require('better-sqlite3');

const ARQ = process.argv[2];
const APLICAR = process.argv[3] === 'aplicar';
const DDD_PADRAO = '31';
const env = Object.assign({}, process.env);
if (!env.EVO_KEY) {
  const pid = execSync('pm2 pid xr-crm').toString().trim();
  fs.readFileSync('/proc/' + pid + '/environ', 'utf8').split('\0').forEach(l => { const i = l.indexOf('='); if (i > 0 && /^EVO_/.test(l)) env[l.slice(0, i)] = l.slice(i + 1); });
}
const BASE = env.EVO_BASE || 'http://172.18.0.1:8085';
const INST = env.EVO_INSTANCE || 'xr-whatsapp';
function post(caminho, corpo) {
  return new Promise((resolve, reject) => {
    const u = new URL(BASE + caminho);
    const p = JSON.stringify(corpo || {});
    const r = http.request({ hostname: u.hostname, port: u.port, path: u.pathname, method: 'POST', headers: { 'Content-Type': 'application/json', apikey: env.EVO_KEY || '', 'Content-Length': Buffer.byteLength(p) } }, res => {
      let d = ''; res.on('data', c => d += c); res.on('end', () => { let j = null; try { j = JSON.parse(d); } catch (e) {} resolve({ status: res.statusCode, json: j }); });
    });
    r.on('error', reject); r.setTimeout(60000, () => { r.destroy(); reject(new Error('demorou')); });
    r.write(p); r.end();
  });
}
const espera = ms => new Promise(ok => setTimeout(ok, ms));

// Número da agenda → formato 55 + DDD + número (ou null se não dá para saber)
function normaliza(d) {
  d = String(d || '').replace(/\D/g, '');
  if (d.startsWith('0')) { d = d.replace(/^0+/, ''); if (d.length === 12 || d.length === 13) d = d.slice(2); } // 0 + operadora + DDD + número
  if (d.startsWith('55') && (d.length === 12 || d.length === 13)) return d;
  if (d.length === 10 || d.length === 11) return '55' + d;
  if (d.length === 8 || d.length === 9) return '55' + DDD_PADRAO + d;   // sem DDD: Belo Horizonte
  if (d.length >= 11 && d.length <= 15 && !d.startsWith('55')) return d; // já com código de outro país
  return null;
}
// O mesmo celular com e sem o nono dígito
function variantes(n) {
  const v = [n];
  if (n.startsWith('55') && n.length === 13 && n[4] === '9') v.push(n.slice(0, 4) + n.slice(5));
  if (n.startsWith('55') && n.length === 12 && /[6-9]/.test(n[4])) v.push(n.slice(0, 4) + '9' + n.slice(4));
  return v;
}

(async () => {
  const agenda = JSON.parse(fs.readFileSync(ARQ, 'utf8'));
  const db = new Database('crm.db');
  db.pragma('busy_timeout = 8000');
  db.exec("CREATE TABLE IF NOT EXISTS agenda (telefone TEXT PRIMARY KEY, nome TEXT NOT NULL, importado_em TEXT DEFAULT (datetime('now','localtime')))");
  try { db.exec("ALTER TABLE contatos ADD COLUMN nome_antes_agenda TEXT DEFAULT ''"); } catch (e) {}

  const crm = new Map(db.prepare('SELECT id, telefone, nome FROM contatos').all().map(c => [c.telefone, c]));
  let conhecidos = new Set();
  try {
    const r = await post('/chat/findContacts/' + INST, { where: {} });
    if (Array.isArray(r.json)) conhecidos = new Set(r.json.filter(c => /@s\.whatsapp\.net$/.test(String(c.remoteJid || ''))).map(c => c.remoteJid.split('@')[0]));
  } catch (e) { console.log('não consegui ler os contatos da Evolution:', e.message); }

  const cont = { total: agenda.length, semNumero: 0, peloCrm: 0, pelaEvolution: 0, perguntados: 0, semWhatsapp: 0, repetidos: 0 };
  const certo = new Map();   // telefone do WhatsApp → nome
  const perguntar = [];      // [{n, nome}]
  const poe = (tel, nome) => {
    const ja = certo.get(tel);
    if (ja !== undefined) { cont.repetidos++; if (nome.length <= ja.length) return; } // mesmo número em dois cartões: fica o nome mais completo
    certo.set(tel, nome);
  };
  for (const c of agenda) {
    const nome = String(c.nome || '').trim().slice(0, 80);
    const n = normaliza((c.tels || [])[0]);
    if (!n || !nome) { cont.semNumero++; continue; }
    const vs = variantes(n);
    const noCrm = vs.find(v => crm.has(v));
    if (noCrm) { cont.peloCrm++; poe(noCrm, nome); continue; }
    const naEvo = vs.find(v => conhecidos.has(v));
    if (naEvo) { cont.pelaEvolution++; poe(naEvo, nome); continue; }
    perguntar.push({ n, nome });
  }
  console.log('agenda:', cont.total, 'contatos | sem número aproveitável:', cont.semNumero, '| achados no CRM:', cont.peloCrm, '| achados nos contatos do WhatsApp:', cont.pelaEvolution, '| a perguntar ao WhatsApp:', perguntar.length);

  if (APLICAR || process.argv[3] === 'perguntar') {
    for (let i = 0; i < perguntar.length; i += 40) {
      const lote = perguntar.slice(i, i + 40);
      try {
        const r = await post('/chat/whatsappNumbers/' + INST, { numbers: lote.map(x => x.n) });
        const resp = Array.isArray(r.json) ? r.json : [];
        for (const x of lote) {
          const a = resp.find(y => y && String(y.number) === x.n);
          if (a && a.exists && /@s\.whatsapp\.net$/.test(String(a.jid || ''))) { cont.perguntados++; poe(String(a.jid).split('@')[0], x.nome); }
          else cont.semWhatsapp++;
        }
      } catch (e) { console.log('lote', i, 'falhou:', e.message); cont.semWhatsapp += lote.length; }
      if (i + 40 < perguntar.length) await espera(2500);
    }
    console.log('confirmados pelo WhatsApp:', cont.perguntados, '| sem WhatsApp (ficam de fora):', cont.semWhatsapp);
  }

  // O nome do CRM fica quando é a versão mais completa do nome da agenda (CRM "Ana Paula Souza" x agenda "Ana Paula").
  // Nos outros casos vale o nome da agenda, que é o apelido que a recepção deu (ex.: "Fulana (Laboratório X)").
  const chave = s => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const crmMaisCompleto = (doCrm, daAgenda) => { const a = chave(doCrm), b = chave(daAgenda); return !!b && a.length > b.length && (' ' + a + ' ').includes(' ' + b + ' '); };
  let trocam = 0, iguais = 0, semNomeAntes = 0, fora = 0, ficam = 0;
  const mudar = new Map(); // id do contato → nome novo
  for (const [tel, nome] of certo) {
    const c = crm.get(tel);
    if (!c) { fora++; continue; }
    if (chave(c.nome) === chave(nome)) iguais++;
    else if (crmMaisCompleto(c.nome, nome)) ficam++;
    else {
      trocam++;
      mudar.set(c.id, nome);
      if (!c.nome) semNomeAntes++;
      if (process.env.AMOSTRA && trocam % 5 === 0) console.log('   amostra: CRM', JSON.stringify(String(c.nome).slice(0, 26)), '→ agenda', JSON.stringify(nome.slice(0, 40)));
    }
  }
  console.log('números aproveitados:', certo.size, '(cartões com número repetido:', cont.repetidos + ')');
  console.log('já têm conversa no CRM:', trocam + iguais + ficam, '→ nome igual:', iguais, '| CRM já tinha o nome mais completo (fica):', ficam, '| nome muda para o da agenda:', trocam, '(desses, estavam sem nome:', semNomeAntes + ')');
  console.log('ainda sem conversa no CRM (aparecem só na busca):', fora);

  if (APLICAR) {
    const poeAgenda = db.prepare("INSERT INTO agenda (telefone, nome) VALUES (?, ?) ON CONFLICT(telefone) DO UPDATE SET nome = excluded.nome, importado_em = datetime('now','localtime')");
    const guarda = db.prepare("UPDATE contatos SET nome_antes_agenda = CASE WHEN COALESCE(nome_antes_agenda,'') = '' THEN COALESCE(nome,'') ELSE nome_antes_agenda END, nome = ? WHERE id = ?");
    db.transaction(() => {
      for (const [tel, nome] of certo) poeAgenda.run(tel, nome);
      for (const [id, nome] of mudar) guarda.run(nome, id);
    })();
    console.log('APLICADO: agenda com', db.prepare('SELECT COUNT(*) n FROM agenda').get().n, 'contatos;', trocam, 'nomes atualizados no CRM (nome anterior guardado).');
  } else {
    console.log('SÓ MOSTREI (nada gravado)');
  }
})().catch(e => console.log('ERRO', e.message));
