'use strict';

// ─── Alerta de follow-up pelo WhatsApp ─────────────────────────────────────────
// Sai pelo número XR Staff (instância xr-staff, chip (31) 98374-6380, o mesmo dos avisos
// de líquor e de laudos) e chega no WhatsApp da recepção. É o único lugar do CRM que
// DISPARA mensagem automática. Até 11/11/2026 saía pelo PrevenVet (xr-prevenvet), que
// ficou desconectado de 16/09 a 11/11 sem ninguém perceber; trocado a pedido do Thiago.
//
// REGRA INEGOCIÁVEL: este módulo só fala com UM número — o da recepção.
// Não é configuração, é trava: qualquer tentativa de mandar para outro número
// é recusada antes de sair. Cliente nunca recebe nada daqui.

const http = require('http');

const EVO_BASE = process.env.EVO_BASE || 'http://172.18.0.1:8085';
const EVO_KEY = process.env.EVO_KEY || '';
const ALERTA_INSTANCE = process.env.ALERTA_INSTANCE || 'xr-staff';

// Lista branca. Um número. Só.
const DESTINO = '553182043117'; // WhatsApp da recepção XR
const ORIGEM  = process.env.ALERTA_ORIGEM || '553183746380'; // número XR Staff — de onde o alerta sai (e onde a recepção responde os comandos)
const CRM_URL = 'https://crm.xrdiagnostico.tech';

// 12/11/2026: DESLIGADO por padrão, a pedido do Thiago. A recepção passou a responder pelo CRM, onde a aba
// Follow-up mostra os mesmos casos (cliente esperando, orçamento sem retorno); o aviso no WhatsApp virou repetição.
// Para religar: variável ALERTA_RECEPCAO=1 no ambiente do xr-crm (pm2 restart xr-crm --update-env).
const LIGADO = process.env.ALERTA_RECEPCAO === '1';

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
  if (!LIGADO) return false; // avisos desligados: nada sai
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
// Formato que o WhatsApp reconhece como número tocável. Muitos JIDs vêm sem o 9º dígito.
const fmtTel = t => {
  const d = String(t || '').replace(/\D/g, '').replace(/^55/, '');
  if (d.length === 11) return `+55 (${d.slice(0, 2)}) ${d.slice(2, 7)}-${d.slice(7)}`;
  if (d.length === 10) return `+55 (${d.slice(0, 2)}) ${d.slice(2, 6)}-${d.slice(6)}`;
  return d ? `+55 ${d}` : '';
};
const nomeDe = c => (c.nome || fmtTel(c.telefone)) + (c.pet_nome ? ` · ${c.pet_nome}` : '');
const ICONE = { 1: '🔴', 2: '🟡', 3: '⚪' };

// ─── Alerta imediato: só 🔴 (cliente esperando resposta) ───────────────────────
// 🟡 e ⚪ não interrompem ninguém — vão no digest.
function notificarNovos(db, subiram, emHorarioUtil) {
  if (!LIGADO) return; // avisos desligados: nem marca como avisado
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
function montarDigest(db, opts = {}) {
  const fila = db.prepare(`
    SELECT id, nome, telefone, pet_nome, followup_nivel, followup_motivo
    FROM contatos WHERE followup_nivel > 0
    ORDER BY followup_nivel ASC, followup_desde ASC
  `).all();

  // Idade em horas a partir do "há Xd / Xh / Xmin" do motivo — pra ordenar do mais antigo pro mais novo.
  const idadeH = c => { const m = String(c.followup_motivo || '').match(/há (\d+)(min|h|d)/); if (!m) return 0; const n = Number(m[1]); return m[2] === 'd' ? n * 24 : m[2] === 'h' ? n : n / 60; };
  const rotuloIdade = c => { const h = idadeH(c); return h >= 48 ? `${Math.floor(h / 24)} dias` : h >= 24 ? '1 dia' : h >= 1 ? `${Math.floor(h)}h` : 'menos de 1h'; };

  const por = { 1: [], 2: [], 3: [] };
  for (const c of fila) por[c.followup_nivel].push(c);
  for (const n of [1, 2, 3]) por[n].sort((a, b) => idadeH(b) - idadeH(a));

  // À tarde, ⚪ (48h+) não repete — quem está mudo há dias já apareceu de manhã.
  const niveis = opts.semSegundaTentativa ? [1, 2] : [1, 2, 3];

  const hoje = new Date().toLocaleDateString('pt-BR', { weekday: 'short', day: '2-digit', month: '2-digit' });

  // Devolve uma LISTA de mensagens: uma por nível (🔴 primeiro), cada uma com
  // no máximo LINHAS_POR_MSG contatos — o resto vai em "(continuação)".
  // Cada linha tem telefone (tocável no WhatsApp) e #id (pros comandos).
  const visiveis = niveis.reduce((s, n) => s + por[n].length, 0);
  if (!visiveis) {
    return [`✅ *Follow-up ${hoje}*\n\nNenhum orçamento parado. Fila limpa! 🎉\n\n_${CRM_URL}_`];
  }

  const LINHAS_POR_MSG = 15;
  const linha = c => `• *${nomeDe(c)}* — ${fmtTel(c.telefone)}  #${c.id}`;

  const titulos = {
    1: '🔴 *CLIENTE ESPERANDO RESPOSTA*',
    2: '🟡 *ORÇAMENTO SEM RETORNO — mandar a 1ª mensagem*',
    3: '⚪ *SEGUE MUDO — 2ª ou última mensagem*',
  };

  const msgs = [];
  for (const n of niveis) {
    const lista = por[n];
    if (!lista.length) continue;
    for (let i = 0; i < lista.length; i += LINHAS_POR_MSG) {
      const pedaco = lista.slice(i, i + LINHAS_POR_MSG);
      const cont = i > 0 ? ` (continuação ${Math.floor(i / LINHAS_POR_MSG) + 1})` : '';
      const cab = msgs.length === 0 ? `📋 *Follow-up ${hoje}* — ${visiveis} na fila\n\n` : '';
      // Agrupado por idade (mais antigo primeiro): "— 4 dias —", "— 3 dias —", "— 22h —"
      let corpo = '', grupoAtual = null;
      for (const c of pedaco) {
        const g = rotuloIdade(c);
        if (g !== grupoAtual) { corpo += `${corpo ? '\n' : ''}_— sem resposta há ${g} —_\n`; grupoAtual = g; }
        corpo += linha(c) + '\n';
      }
      msgs.push(`${cab}${titulos[n]} (${lista.length})${cont}\n${corpo.trimEnd()}`);
    }
  }
  msgs[msgs.length - 1] += `\n\nA mensagem pronta de cada orçamento está no card, na aba Follow-up do CRM.\nRespondeu o cliente? Some sozinho.\nPor aqui: *marcou 123* · *desistiu 123* · *ok 123* · *ajuda*\n${CRM_URL}`;
  return msgs;
}

// Envia uma lista de mensagens em sequência, com um respiro entre elas.
async function enviarSequencia(msgs) {
  for (let i = 0; i < msgs.length; i++) {
    await enviarRecepcao(msgs[i]);
    if (i < msgs.length - 1) await new Promise(r => setTimeout(r, 2500));
  }
}

function iniciarDigest(db, emHorarioUtil) {
  let ultimoEnvio = ''; // 'YYYY-MM-DD-H' para não repetir no mesmo slot

  const tick = () => {
    if (!LIGADO) return; // avisos desligados
    const agora = new Date();
    const h = agora.getHours(), m = agora.getMinutes();
    const dia = agora.getDay();
    if (dia === 0) return; // domingo não

    const slot = (h === HORA_DIGEST_MANHA && m >= 5) ? 'manha' : (h === HORA_DIGEST_TARDE && m >= 5) ? 'tarde' : null;
    if (!slot) return;
    const chave = `${agora.toISOString().slice(0, 10)}-${slot}`;
    if (chave === ultimoEnvio) return;
    ultimoEnvio = chave;

    const msgs = montarDigest(db, { semSegundaTentativa: slot === 'tarde' }); // ⚪ só de manhã
    const vazio = msgs.length === 1 && msgs[0].startsWith('✅');
    if (slot === 'tarde' && vazio) return; // à tarde só se tiver o que cobrar
    enviarSequencia(msgs);                 // de manhã sempre — é o heartbeat
  };

  tick();
  return setInterval(tick, 60 * 1000);
}

module.exports = { LIGADO, DESTINO, ORIGEM, INSTANCIA: ALERTA_INSTANCE, enviarRecepcao, enviarSequencia, notificarNovos, montarDigest, iniciarDigest };
