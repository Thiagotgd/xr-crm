'use strict';

// ─── Motor de follow-up do funil de orçamentos ─────────────────────────────────
// Vigia quem recebeu orçamento (PDF) e não fechou. Marca nível no banco.
// FASE 1: só marca. Não envia mensagem para ninguém. O envio mora em alerta.js.

const ABERTURA = 8;      // expediente começa 8h
const FECHAMENTO = 18;   // expediente termina 18h
// dias úteis: seg(1) a sáb(6). domingo(0) não conta.

const REGRAS = {
  minutosClienteEsperando: 30,   // 🔴 cliente falou por último e ninguém respondeu
  horasSemResposta: 12,          // 🟡 mandamos e o cliente sumiu
  horasSegundaTentativa: 48,     // ⚪ segunda tentativa depois do 🟡

  // Nada anterior a esta data é cobrado. O histórico velho fica visível no CRM,
  // mas em silêncio: orçamento de meses atrás pode ser paciente que já morreu.
  corte: '2026-08-17 00:00:00',

  // Mensagem curta do cliente ("ok", "obrigado", emoji) encerra conversa e não
  // é alguém esperando resposta. Não vira 🔴 — cai na régua normal de 12h.
  tamanhoCortesia: 10,
};

// Se qualquer uma destas palavras aparecer na conversa, o contato nunca é
// cobrado — em nenhum nível, para sempre. Vale mais deixar passar uma venda
// do que perguntar "fechou o orçamento?" para quem perdeu o animal.
const PALAVRAS_LUTO = [
  'obito', 'óbito', 'faleceu', 'falecido', 'falecida', 'morreu', 'morreram',
  'eutanasia', 'eutanásia', 'sacrificado', 'sacrificada', 'nao resistiu',
  'não resistiu', 'veio a obito', 'veio a óbito', 'descansou', 'partiu ontem',
  'perdemos ele', 'perdemos ela', 'perdi ele', 'perdi ela', 'meus pesames',
  'meus pêsames', 'sentimos muito pela perda',
];

// Cliente disse que não vai fazer. Sai da fila de vez: o contato vai para
// "Finalizado" e o follow-up zera. Só vale para mensagem DO CLIENTE chegada
// depois do orçamento — nunca para texto nosso.
const PALAVRAS_DESISTENCIA = [
  'nao quero mais', 'não quero mais', 'nao vou fazer', 'não vou fazer',
  'nao vamos fazer', 'não vamos fazer', 'desisti', 'desistimos',
  'ja fiz em outro', 'já fiz em outro', 'fiz em outro lugar',
  'ja marquei em outro', 'já marquei em outro', 'marquei em outro lugar',
  'vou deixar pra depois', 'vou deixar para depois', 'deixa pra outra hora',
  'nao tenho condicoes', 'não tenho condições', 'muito caro pra mim',
  'nao vai ser possivel', 'não vai ser possível',
];

// A Athena (atendente virtual fora do horário) responde pelo MESMO número da
// recepção, então as mensagens dela entram como 'out'. Para a régua, Athena
// não conta como resposta humana: cliente que escreveu de noite e só falou
// com ela continua "esperando resposta" e vira 🔴 na primeira meia hora útil.
// Toda mensagem dela começa com este marcador (athena-bot/server.js → enviar()).
const ATHENA_MARCA = 'Assistente virtual Athena';

// ─── Horário útil ──────────────────────────────────────────────────────────────
const ehDiaUtil = d => d.getDay() >= 1 && d.getDay() <= 6;
const emHorarioUtil = d => ehDiaUtil(d) && d.getHours() >= ABERTURA && d.getHours() < FECHAMENTO;

// Próximo instante em que o expediente está aberto (hoje mais tarde, ou o próximo dia útil).
function proximaAbertura(d) {
  const x = new Date(d.getTime());
  if (ehDiaUtil(x) && x.getHours() < ABERTURA) { x.setHours(ABERTURA, 0, 0, 0); return x; }
  do { x.setDate(x.getDate() + 1); } while (!ehDiaUtil(x));
  x.setHours(ABERTURA, 0, 0, 0);
  return x;
}

// Se o prazo cair fora do expediente, empurra para a primeira hora útil seguinte.
// É a regra "12h ou na primeira hora útil do dia seguinte".
const quandoAlertar = d => emHorarioUtil(d) ? new Date(d.getTime()) : proximaAbertura(d);

// Minutos de expediente decorridos entre dois instantes (ignora noite e domingo).
function minutosUteisEntre(ini, fim) {
  let total = 0;
  let cur = new Date(ini.getTime());
  while (cur < fim) {
    if (emHorarioUtil(cur)) {
      const fimExpediente = new Date(cur.getTime());
      fimExpediente.setHours(FECHAMENTO, 0, 0, 0);
      const ate = fim < fimExpediente ? fim : fimExpediente;
      total += (ate - cur) / 60000;
      cur = ate;
      if (cur >= fim) break;
    } else {
      cur = proximaAbertura(cur);
    }
  }
  return Math.floor(total);
}

// ─── Datas no formato do banco ('YYYY-MM-DD HH:MM:SS', horário local) ──────────
function dt(s) {
  if (!s) return null;
  const d = new Date(String(s).replace(' ', 'T'));
  return isNaN(d.getTime()) ? null : d;
}
function sql(d) {
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
function humano(min) {
  if (min < 60) return `${min}min`;
  const h = Math.floor(min / 60);
  return h < 24 ? `${h}h` : `${Math.floor(h / 24)}d`;
}

// ─── Migrations ────────────────────────────────────────────────────────────────
function initFollowup(db) {
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_nivel INTEGER DEFAULT 0"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_desde TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_motivo TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_alertado_em TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN orcamento_modalidade TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_snooze_ate TEXT DEFAULT ''"); } catch {}
  try { db.exec("CREATE INDEX IF NOT EXISTS idx_contatos_followup ON contatos(followup_nivel)"); } catch {}
}

// ─── Correção do bug do laudo ──────────────────────────────────────────────────
// O laudo também sai em PDF, depois do exame. O auto-move tratava esse PDF como
// orçamento novo e jogava o contato de "Agendado" de volta para "Orçamento".
// Devolve para "Agendado" quem entrou em Orçamento vindo de Agendado.
function corrigirLaudos(db) {
  const ultimaTransicao = db.prepare(`
    SELECT etapa_de FROM etapa_historico
    WHERE contato_id = ? ORDER BY id DESC LIMIT 1
  `);
  const alvos = db.prepare("SELECT id, nome, telefone FROM contatos WHERE etapa = 'orcamento'").all();

  let corrigidos = 0;
  const voltar = db.prepare("UPDATE contatos SET etapa = 'agendado', followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE id = ?");
  const logar = db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'orcamento', 'agendado')");

  db.transaction(() => {
    for (const c of alvos) {
      const t = ultimaTransicao.get(c.id);
      if (t && t.etapa_de === 'agendado') {
        voltar.run(c.id);
        logar.run(c.id);
        corrigidos++;
      }
    }
  })();

  if (corrigidos) console.log(`[CRM] Follow-up: ${corrigidos} contatos devolvidos para Agendado (PDF era laudo, não orçamento)`);
  return corrigidos;
}

// ─── Encerramento automático ───────────────────────────────────────────────────
// Varre quem está em Orçamento: se o CLIENTE, depois do orçamento, disse que
// desistiu ou que o animal morreu, o contato sai da fila (vai para Finalizado)
// e nunca mais é cobrado. É o que fecha o ciclo do follow-up: o time cobra,
// o cliente responde "não quero mais" / "ele faleceu", e o card some sozinho.
function encerrarPerdidos(db) {
  const mk = lista => ({
    cond: lista.map(() => 'LOWER(texto) LIKE ?').join(' OR '),
    args: lista.map(p => `%${p}%`),
  });
  const luto = mk(PALAVRAS_LUTO);
  const desist = mk(PALAVRAS_DESISTENCIA);

  const acha = (c, m) => db.prepare(
    `SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'in'
     AND created_at >= ? AND (${m.cond}) LIMIT 1`
  ).get(c.id, c.orcamento_enviado_at || c.created_at || '', ...m.args);

  const alvos = db.prepare("SELECT id, nome, telefone, etapa, orcamento_enviado_at, created_at FROM contatos WHERE etapa = 'orcamento'").all();
  const finalizar = db.prepare("UPDATE contatos SET etapa = 'finalizado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?");
  const logar = db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'orcamento', 'finalizado')");

  const encerrados = [];
  db.transaction(() => {
    for (const c of alvos) {
      const porLuto = acha(c, luto);
      const porDesist = !porLuto && acha(c, desist);
      if (porLuto || porDesist) {
        finalizar.run(c.id);
        logar.run(c.id);
        encerrados.push({ id: c.id, nome: c.nome, telefone: c.telefone, motivo: porLuto ? 'luto' : 'desistiu' });
      }
    }
  })();

  for (const e of encerrados) console.log(`[CRM] Follow-up: ${e.nome || e.telefone} encerrado (${e.motivo})`);
  return encerrados;
}

// ─── Decisão ───────────────────────────────────────────────────────────────────
// Função pura: dado o contato e a última mensagem da conversa, qual é o nível.
// Separada do banco de propósito — é isso que permite simular em cima do
// histórico de produção sem escrever nada.
function decidir(contato, ultimaMsg, agora = new Date()) {
  const nada = (por) => ({ nivel: 0, motivo: '', desde: null, ignoradoPor: por || '' });
  if (!ultimaMsg) return nada('sem mensagem');

  // Luto: nunca cobra, em nenhuma hipótese.
  if (contato.luto) return nada('luto');

  // Corte: conversa parada desde antes do corte fica em silêncio.
  const quandoUltima = dt(ultimaMsg.created_at);
  const corte = dt(REGRAS.corte);
  if (corte && quandoUltima && quandoUltima < corte) return nada('anterior ao corte');

  if (ultimaMsg.direcao === 'in') {
    // Cliente falou por último. A bola é nossa — a menos que tenha sido só cortesia.
    if (!quandoUltima) return nada('data inválida');
    const cortesia = ultimaMsg.tipo === 'text' && (ultimaMsg.tamanho || 0) <= REGRAS.tamanhoCortesia;
    if (!cortesia) {
      const min = minutosUteisEntre(quandoUltima, agora);
      if (min < REGRAS.minutosClienteEsperando) return nada('dentro do prazo');
      return { nivel: 1, motivo: `cliente esperando resposta há ${humano(min)}`, desde: quandoUltima, ignoradoPor: '' };
    }
    // Cortesia curta: segue para a régua de 12h abaixo, sem urgência.
  }

  // Falamos por último. A bola é do cliente — mas o toque é nosso.
  const base = dt(ultimaMsg.created_at);
  const orc = dt(contato.orcamento_enviado_at);
  const ref = (orc && base && orc > base) ? orc : base;
  if (!ref) return nada('data inválida');

  const alvo = quandoAlertar(new Date(ref.getTime() + REGRAS.horasSemResposta * 3600000));
  if (agora < alvo) return nada('dentro do prazo');

  const alvo2 = new Date(alvo.getTime() + REGRAS.horasSegundaTentativa * 3600000);
  const nivel = agora >= alvo2 ? 3 : 2;
  const min = minutosUteisEntre(ref, agora);
  return {
    nivel,
    motivo: nivel === 3
      ? `sem resposta há ${humano(min)} — segunda tentativa`
      : `orçamento sem resposta há ${humano(min)}`,
    desde: alvo,
    ignoradoPor: '',
  };
}

// ─── Avaliação ─────────────────────────────────────────────────────────────────
// Recalcula o nível de todo mundo do zero a cada rodada. Idempotente de propósito:
// sem estado acumulado, sem risco de o contador desandar com o tempo.
function avaliar(db, agora = new Date()) {
  // Números internos da XR nunca entram na fila (recepção falando com ela mesma etc.).
  const internos = ['553182043117', '553183283626', '553183746380'];
  const alvos = db.prepare(`
    SELECT id, nome, telefone, orcamento_enviado_at, followup_nivel, followup_snooze_ate
    FROM contatos
    WHERE etapa = 'orcamento' AND orcamento_enviado_at != ''
  `).all().filter(c => !internos.includes(String(c.telefone).replace(/\D/g, '')));

  // Última mensagem HUMANA da conversa: ignora o que a Athena mandou.
  const ultimaMsg = db.prepare(`
    SELECT direcao, created_at, tipo, LENGTH(texto) AS tamanho FROM mensagens
    WHERE contato_id = ? AND NOT (direcao = 'out' AND texto LIKE ?)
    ORDER BY id DESC LIMIT 1`);
  const athenaLike = '%' + ATHENA_MARCA + '%';
  const condLuto = PALAVRAS_LUTO.map(() => "LOWER(texto) LIKE ?").join(" OR ");
  const temLuto = db.prepare(`SELECT 1 FROM mensagens WHERE contato_id = ? AND (${condLuto}) LIMIT 1`);
  const argsLuto = PALAVRAS_LUTO.map(p => `%${p}%`);
  const gravar = db.prepare("UPDATE contatos SET followup_nivel = ?, followup_desde = ?, followup_motivo = ? WHERE id = ?");

  const mudancas = [];

  for (const c of alvos) {
    const m = ultimaMsg.get(c.id, athenaLike);
    c.luto = !!temLuto.get(c.id, ...argsLuto);
    let { nivel, motivo, desde } = decidir(c, m, agora);
    // "ok 123" pela recepção: silencia por um tempo (já ligaram, já resolveram por fora).
    if (nivel > 0 && c.followup_snooze_ate && dt(c.followup_snooze_ate) && agora < dt(c.followup_snooze_ate)) { nivel = 0; motivo = ''; desde = null; }

    if (nivel !== c.followup_nivel) {
      gravar.run(nivel, desde ? sql(desde) : '', motivo, c.id);
      mudancas.push({ id: c.id, nome: c.nome, telefone: c.telefone, de: c.followup_nivel, para: nivel, motivo });
    } else if (nivel > 0) {
      gravar.run(nivel, desde ? sql(desde) : '', motivo, c.id); // atualiza o texto do "há Xh"
    }
  }

  // Internos nunca ficam marcados.
  for (const n of internos) db.prepare("UPDATE contatos SET followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE followup_nivel > 0 AND REPLACE(REPLACE(REPLACE(telefone,'+',''),'-',''),' ','') LIKE ?").run('%' + n.slice(-11) + '%');

  // Quem saiu de Orçamento (fechou, arquivou) para de ser cobrado.
  db.prepare("UPDATE contatos SET followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE followup_nivel > 0 AND etapa != 'orcamento'").run();

  return mudancas;
}

// ─── Vigia ─────────────────────────────────────────────────────────────────────
function iniciarVigia(db, opts = {}) {
  const intervaloMs = opts.intervaloMs || 5 * 60 * 1000;
  const aoMudar = opts.aoMudar || null;

  const rodada = () => {
    try {
      encerrarPerdidos(db);
      const mudancas = avaliar(db);
      const subiram = mudancas.filter(m => m.para > m.de);
      const icone = n => n === 1 ? '🔴' : n === 2 ? '🟡' : '⚪';
      for (const m of subiram) console.log(`[CRM] Follow-up ${icone(m.para)} ${m.nome || m.telefone}: ${m.motivo}`);
      if (aoMudar && subiram.length) aoMudar(subiram);
    } catch (e) {
      console.error('[CRM] Follow-up erro na rodada:', e.message);
    }
  };

  rodada();
  return setInterval(rodada, intervaloMs);
}

// ─── Comandos da recepção (respondidos na conversa do robô) ────────────────────
function executarComando(db, cmd, id) {
  if (cmd === 'ajuda' || cmd === '?') {
    return '🤖 *Comandos do follow-up*\n\n' +
      '• *perdeu 123* — cliente desistiu / não vai fazer → sai da fila\n' +
      '• *fechou 123* — marcou o exame → vai pra Agendado\n' +
      '• *ok 123* — já resolvi por fora (liguei etc.) → silencia por 24h\n\n' +
      'O número é o *#id* que aparece no alerta.';
  }
  // "ok" / "feito" soltos são conversa normal — ignora em silêncio. "perdeu" solto, avisa.
  if (!id && (cmd === 'ok' || cmd === 'feito' || cmd === 'resolvido')) return null;
  if (!id) return `Faltou o número. Ex.: *${cmd} 123* (o #id que vem no alerta). Mande *ajuda* pra ver os comandos.`;

  const c = db.prepare('SELECT id, nome, telefone, pet_nome, etapa FROM contatos WHERE id = ?').get(id);
  if (!c) return `Não achei o contato *#${id}*. Confere o número no alerta.`;
  const quem = (c.nome || c.telefone) + (c.pet_nome ? ` · ${c.pet_nome}` : '');

  if (cmd === 'perdeu' || cmd === 'perdido') {
    db.prepare("UPDATE contatos SET etapa = 'finalizado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
    db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'finalizado')").run(id, c.etapa);
    console.log(`[CRM] Comando perdeu #${id} (${quem})`);
    return `✔️ *${quem}* (#${id}) marcado como perdido. Saiu da fila.`;
  }
  if (cmd === 'fechou' || cmd === 'agendou') {
    db.prepare("UPDATE contatos SET etapa = 'agendado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
    db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'agendado')").run(id, c.etapa);
    console.log(`[CRM] Comando fechou #${id} (${quem})`);
    return `🎉 *${quem}* (#${id}) foi pra Agendado. Boa!`;
  }
  const ate = new Date(Date.now() + 24 * 3600000);
  db.prepare("UPDATE contatos SET followup_snooze_ate = ?, followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE id = ?").run(sql(ate), id);
  console.log(`[CRM] Comando ok #${id} (${quem}) snooze até ${sql(ate)}`);
  return `👍 *${quem}* (#${id}) silenciado por 24h. Se o cliente responder, volta sozinho.`;
}

module.exports = {
  executarComando,
  REGRAS, ABERTURA, FECHAMENTO,
  emHorarioUtil, proximaAbertura, quandoAlertar, minutosUteisEntre,
  initFollowup, corrigirLaudos, encerrarPerdidos, decidir, avaliar, iniciarVigia,
  PALAVRAS_LUTO, PALAVRAS_DESISTENCIA, ATHENA_MARCA,
};
