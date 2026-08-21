'use strict';

// ─── Alerta de follow-up pelo WhatsApp ─────────────────────────────────────────
// Sai pelo número do PrevenVet (instância xr-prevenvet) e chega no WhatsApp da
// recepção. É o único lugar do CRM que DISPARA mensagem automática.
//
// REGRA INEGOCIÁVEL: este módulo só fala com UM número — o da recepção.
// Não é configuração, é trava: qualquer tentativa de mandar para outro número
// é recusada antes de sair. Cliente nunca recebe nada daqui.

const http = require('http');

const EVO_BASE = process.env.EVO_BASE || 'http://172.18.0.1:8085';
const EVO_KEY = process.env.EVO_KEY || '';
const ALERTA_INSTANCE = process.env.ALERTA_INSTANCE || 'xr-prevenvet';

// Lista branca. Um número. Só.
const DESTINO = '553182043117'; // WhatsApp da recepção XR
const ORIGEM  = '553183283626'; // número do PrevenVet — de onde o alerta sai (e onde a recepção responde)
const CRM_URL = 'https://crm.xrdiagnostico.tech';

const HORA_DIGEST_MANHA = 8;    // 08:05 — sempre envia, mesmo vazio (é o heartbeat)
const HORA_DIGEST_TARDE = 13;   // 13:05 — só se tiver pendência

// ─── Envio (com a trava) ───────────────────────────────────────────────────────
function enviar(numero, texto) {
  return new Promise((resolve, reject) => {
    if (String(numero) !== DESTINO) {
      return reject(new Error(`[ALERTA] BLOQUEADO: tentativa de envio para ${numero} (só ${DESTINO} é permitido)`));
    }
    if (!EVO_KEY) return reject(new Error('[ALERTA] EVO_KEY vazio'));

    const body = JSON.stringify({ number: DESTINO, text: texto });
    const url = new URL(`${EVO_BASE}/message/sendText/${ALERTA_INSTANCE}`);
    const req = http.request({
      hostname: url.hostname, port: url.port, path: url.pathname, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body), apikey: EVO_KEY },
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) resolve(true);
        else reject(new Error(`[ALERTA] Evolution HTTP ${res.statusCode}: ${data.slice(0, 150)}`));
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('[ALERTA] timeout')); });
    req.write(body);
    req.end();
  });
}

async function enviarRecepcao(texto) {
  try {
    await enviar(DESTINO, texto);
    console.log(`[ALERTA] enviado (${texto.split('\n')[0].slice(0, 50)})`);
    return true;
  } catch (e) {
    console.error('[ALERTA] falha:', e.message);
    return false;
  }
}

// ─── Formatação ────────────────────────────────────────────────────────────────
const fmtTel = t => {
  const d = String(t || '').replace(/\D/g, '').replace(/^55/, '');
  return d.length === 11 ? `(${d.slice(0, 2)}) ${d.slice(2, 7)}-${d.slice(7)}` : d;
};
const nomeDe = c => (c.nome || fmtTel(c.telefone)) + (c.pet_nome ? ` · ${c.pet_nome}` : '');
const ICONE = { 1: '🔴', 2: '🟡', 3: '⚪' };

// ─── Alerta imediato: só 🔴 (cliente esperando resposta) ───────────────────────
// 🟡 e ⚪ não interrompem ninguém — vão no digest.
function notificarNovos(db, subiram, emHorarioUtil) {
  const agora = new Date();
  if (!emHorarioUtil(agora)) return; // fora do expediente, fica pro digest das 8h

  const urgentes = subiram.filter(m => m.para === 1);
  if (!urgentes.length) return;

  const marcar = db.prepare("UPDATE contatos SET followup_alertado_em = datetime('now','localtime') WHERE id = ?");
  const pega = db.prepare('SELECT id, nome, telefone, pet_nome FROM contatos WHERE id = ?');

  const linhas = urgentes.map(u => {
    const c = pega.get(u.id) || u;
    marcar.run(u.id);
    return `🔴 *${nomeDe(c)}*  #${c.id}\n   ${u.motivo}\n   📱 ${fmtTel(c.telefone)}`;
  });

  const cab = urgentes.length === 1 ? 'Cliente esperando resposta' : `${urgentes.length} clientes esperando resposta`;
  const texto = `⚠️ *${cab}*\n\n${linhas.join('\n\n')}\n\n${CRM_URL}`;
  enviarRecepcao(texto);
}

// ─── Digest: a fila inteira, 2x por dia ───────────────────────────────────────
function montarDigest(db) {
  const fila = db.prepare(`
    SELECT id, nome, telefone, pet_nome, followup_nivel, followup_motivo
    FROM contatos WHERE followup_nivel > 0
    ORDER BY followup_nivel ASC, followup_desde ASC
  `).all();

  const por = { 1: [], 2: [], 3: [] };
  for (const c of fila) por[c.followup_nivel].push(c);

  const hoje = new Date().toLocaleDateString('pt-BR', { weekday: 'short', day: '2-digit', month: '2-digit' });

  if (!fila.length) {
    return `✅ *Follow-up ${hoje}*\n\nNenhum orçamento parado. Fila limpa! 🎉\n\n_${CRM_URL}_`;
  }

  const bloco = (n, titulo, max) => {
    if (!por[n].length) return '';
    const itens = por[n].slice(0, max).map(c => `${ICONE[n]} ${nomeDe(c)} — _${c.followup_motivo}_  #${c.id}`);
    const resto = por[n].length > max ? `\n   … e mais ${por[n].length - max}` : '';
    return `*${titulo} (${por[n].length})*\n${itens.join('\n')}${resto}\n\n`;
  };

  return `📋 *Follow-up ${hoje}* — ${fila.length} na fila\n\n` +
    bloco(1, 'Cliente esperando resposta', 12) +
    bloco(2, 'Orçamento sem retorno (12h+)', 10) +
    bloco(3, 'Segunda tentativa (48h+)', 8) +
    `Respondeu o cliente? O card some sozinho.\nPor aqui: *perdeu 123* · *fechou 123* · *ok 123* (ou *ajuda*)\n${CRM_URL}`;
}

function iniciarDigest(db, emHorarioUtil) {
  let ultimoEnvio = ''; // 'YYYY-MM-DD-H' para não repetir no mesmo slot

  const tick = () => {
    const agora = new Date();
    const h = agora.getHours(), m = agora.getMinutes();
    const dia = agora.getDay();
    if (dia === 0) return; // domingo não

    const slot = (h === HORA_DIGEST_MANHA && m >= 5) ? 'manha' : (h === HORA_DIGEST_TARDE && m >= 5) ? 'tarde' : null;
    if (!slot) return;
    const chave = `${agora.toISOString().slice(0, 10)}-${slot}`;
    if (chave === ultimoEnvio) return;
    ultimoEnvio = chave;

    const texto = montarDigest(db);
    const vazio = texto.startsWith('✅');
    if (slot === 'tarde' && vazio) return; // à tarde só se tiver o que cobrar
    enviarRecepcao(texto);                 // de manhã sempre — é o heartbeat
  };

  tick();
  return setInterval(tick, 60 * 1000);
}

module.exports = { DESTINO, ORIGEM, enviarRecepcao, notificarNovos, montarDigest, iniciarDigest };
