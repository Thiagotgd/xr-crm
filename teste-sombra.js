'use strict';
// Testes do modo sombra (sombra.js). Banco em memória e IA de mentira — não chama a ponte nem toca o crm.db.
// Uso: node teste-sombra.js (sai 1 se falhar)
const assert = require('assert');
const Database = require('better-sqlite3');
const followup = require('./followup');
const sombra = require('./sombra');

let falhas = 0;
const fila = [];
const t = (nome, fn) => fila.push([nome, fn]);
const logOriginal = console.log;

function novoBanco() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE contatos (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT DEFAULT '', telefone TEXT DEFAULT '', pet_nome TEXT DEFAULT '', etapa TEXT DEFAULT 'novo',
      equipe_xr INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now','localtime')), updated_at TEXT DEFAULT (datetime('now','localtime')), orcamento_enviado_at TEXT DEFAULT '');
    CREATE TABLE mensagens (id INTEGER PRIMARY KEY AUTOINCREMENT, contato_id INTEGER, direcao TEXT, texto TEXT, tipo TEXT DEFAULT 'text', created_at TEXT);
    CREATE TABLE etapa_historico (id INTEGER PRIMARY KEY AUTOINCREMENT, contato_id INTEGER, etapa_de TEXT, etapa_para TEXT, created_at TEXT DEFAULT (datetime('now','localtime')));
  `);
  followup.initFollowup(db);
  sombra.init(db); sombra.init(db);
  return db;
}
const D = s => new Date(s.replace(' ', 'T'));
const AGORA = D('2026-11-10 16:00:00');
const contato = (db, o = {}) => db.prepare("INSERT INTO contatos (nome, telefone, pet_nome, equipe_xr, followup_ignorar) VALUES (?,?,?,?,?)").run(o.nome || 'Ana', o.tel || '553188887777', o.pet || '', o.equipe || 0, o.ignorar || 0).lastInsertRowid;
const msg = (db, id, dir, texto, quando, tipo) => db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, created_at) VALUES (?,?,?,?,?)").run(id, dir, texto, tipo || 'text', '2026-11-10 ' + quando).lastInsertRowid;

t('turno simples: bloco do cliente + bloco de resposta', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Bom dia', '09:00:00'); msg(db, c, 'in', 'Qual o valor do ultrassom?', '09:00:20');
  msg(db, c, 'out', 'Bom dia!', '09:03:00'); msg(db, c, 'out', 'Ultrassom abdominal - R$ 180,00', '09:03:30');
  const ts = sombra.acharTurnos(db, AGORA);
  assert.strictEqual(ts.length, 1);
  assert.strictEqual(ts[0].cliente, 'Bom dia\nQual o valor do ultrassom?');
  assert.strictEqual(ts[0].real, 'Bom dia!\nUltrassom abdominal - R$ 180,00');
  assert.strictEqual(ts[0].quando, '2026-11-10 09:00:20');
});
t('saudação automática não quebra o bloco nem conta como resposta', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Olá', '09:00:00');
  msg(db, c, 'out', 'XR Diagnóstico agradece seu contato. Como podemos ajudar?', '09:00:01');
  msg(db, c, 'in', 'Vocês fazem tomografia?', '09:00:30');
  msg(db, c, 'out', 'Fazemos sim! Me envia o pedido?', '09:05:00');
  const ts = sombra.acharTurnos(db, AGORA);
  assert.strictEqual(ts.length, 1);
  assert.strictEqual(ts[0].cliente, 'Olá\nVocês fazem tomografia?');
  assert.ok(!ts[0].real.includes('agradece'));
});
t('quem respondeu foi a Athena: turno fora', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Qual o valor da ressonância?', '07:00:00');
  msg(db, c, 'out', '🐾 _Assistente virtual Athena_ 🐾\n\nOi! A recepção envia o orçamento.', '07:00:10');
  assert.strictEqual(sombra.acharTurnos(db, AGORA).length, 0);
});
t('cortesia pura do cliente ("ok obrigada") não vira turno', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'ok, obrigada', '10:00:00'); msg(db, c, 'out', 'Disponha!', '10:01:00');
  assert.strictEqual(sombra.acharTurnos(db, AGORA).length, 0);
});
t('cliente sem resposta ainda: nada a comparar', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Tem horário amanhã?', '15:00:00');
  assert.strictEqual(sombra.acharTurnos(db, AGORA).length, 0);
});
t('resposta ainda em andamento (recepção escreveu há 5 min) espera fechar', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Tem horário amanhã?', '15:50:00'); msg(db, c, 'out', 'Um momento', '15:55:00');
  assert.strictEqual(sombra.acharTurnos(db, AGORA).length, 0);
  assert.strictEqual(sombra.acharTurnos(db, D('2026-11-10 16:20:00')).length, 1);
});
t('resposta que demorou mais de 24h não é resposta àquela mensagem', () => {
  const db = novoBanco(); const c = contato(db);
  db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, created_at) VALUES (?, 'in', 'Qual o valor do raio-x?', '2026-11-09 10:30:00')").run(c);
  msg(db, c, 'out', 'Boa tarde! R$ 180,00', '15:00:00');
  assert.strictEqual(sombra.acharTurnos(db, AGORA).length, 0);
});
t('contato interno, da equipe ou marcado "não é cliente" fica fora', () => {
  const db = novoBanco();
  for (const o of [{ tel: '553182043117' }, { equipe: 1 }, { ignorar: 1 }]) {
    const c = contato(db, o); msg(db, c, 'in', 'Qual o valor do ultrassom?', '09:00:00'); msg(db, c, 'out', 'R$ 180', '09:02:00');
  }
  assert.strictEqual(sombra.acharTurnos(db, AGORA).length, 0);
});
t('imagem, áudio e documento aparecem como marcadores', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', '[image]', '09:00:00', 'image'); msg(db, c, 'in', '[audio]', '09:00:10', 'audio'); msg(db, c, 'in', 'pedido.pdf', '09:00:20', 'document');
  msg(db, c, 'out', 'Orçamento Tomografia - Pet (Tutor).pdf', '09:10:00', 'document');
  const ts = sombra.acharTurnos(db, AGORA);
  assert.strictEqual(ts[0].cliente, '[imagem]\n[áudio]\n[documento: pedido.pdf]');
  assert.strictEqual(ts[0].real, '[documento: Orçamento Tomografia - Pet (Tutor).pdf]');
});
t('dois turnos na mesma conversa', () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Qual o valor do ultrassom?', '09:00:00'); msg(db, c, 'out', 'R$ 180,00', '09:02:00');
  msg(db, c, 'in', 'Tem horário hoje?', '09:03:00'); msg(db, c, 'out', 'Temos às 15h, pode ser?', '09:05:00');
  assert.strictEqual(sombra.acharTurnos(db, AGORA).length, 2);
});
t('lerJson tolera texto em volta e rejeita lixo', () => {
  assert.deepStrictEqual(sombra.lerJson('Claro!\n```json\n{"acao":"responder","mensagem":"oi"}\n```'), { acao: 'responder', mensagem: 'oi' });
  assert.strictEqual(sombra.lerJson('sem json aqui'), null);
  assert.strictEqual(sombra.lerJson('{quebrado'), null);
});
t('prompt do rascunho: tem base, hora da mensagem, conversa e NÃO tem a resposta real', () => {
  const db = novoBanco(); const c = contato(db, { nome: 'Carla', pet: 'Luna' });
  msg(db, c, 'in', 'Qual o valor do ultrassom?', '09:00:00'); msg(db, c, 'out', 'RESPOSTA-SECRETA R$ 180', '09:02:00');
  const turno = sombra.acharTurnos(db, AGORA)[0];
  const p = sombra.promptRascunho(db, turno);
  assert.ok(p.includes('BASE DE CONHECIMENTO') && p.includes('terça-feira, 10/11/2026 às 09:00'));
  assert.ok(p.includes('CLIENTE: Qual o valor do ultrassom?') && p.includes('Pet: Luna'));
  assert.ok(!p.includes('RESPOSTA-SECRETA'), 'o agente não pode ver a resposta da recepção');
  const pj = sombra.promptJuiz(db, turno, { acao: 'responder', mensagem: 'R$ 180,00', motivo: 'x' });
  assert.ok(pj.includes('RESPOSTA-SECRETA') && pj.includes('ação: responder'));
});
t('system prompts cabem no limite da ponte (4000 caracteres)', () => {
  assert.ok(sombra.SYSTEM_RASCUNHO.length < 4000, 'rascunho ' + sombra.SYSTEM_RASCUNHO.length);
  assert.ok(sombra.SYSTEM_JUIZ.length < 4000, 'juiz ' + sombra.SYSTEM_JUIZ.length);
});

const ia = respostas => { let i = 0; const chamadas = []; const f = async (prompt, system, model) => { chamadas.push({ model, system }); const r = respostas[i++]; if (r instanceof Error) throw r; return { texto: r, modelo: model + '-teste' }; }; f.chamadas = chamadas; return f; };

t('avaliarTurno: rascunho com um modelo, comparação com outro, linha completa', async () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Qual o valor do ultrassom?', '09:00:00'); msg(db, c, 'out', 'R$ 180,00', '09:02:00');
  const f = ia(['{"acao":"responder","mensagem":"O ultrassom abdominal é R$ 180,00.","motivo":"preço na base"}',
    '{"categoria":"preco_exame_simples","veredito":"equivalente","erro_grave":false,"humano_necessario":false,"explicacao":"mesmo valor"}']);
  const l = await sombra.avaliarTurno(db, sombra.acharTurnos(db, AGORA)[0], f);
  assert.strictEqual(l.erro, ''); assert.strictEqual(l.acao, 'responder'); assert.strictEqual(l.veredito, 'equivalente'); assert.strictEqual(l.categoria, 'preco_exame_simples');
  assert.strictEqual(f.chamadas[0].model, sombra.REGRAS.modeloRascunho); assert.strictEqual(f.chamadas[1].model, sombra.REGRAS.modeloJuiz);
});
t('avaliarTurno: resposta fora do formato ou ponte fora do ar vira erro registrado, sem derrubar', async () => {
  const db = novoBanco(); const c = contato(db);
  msg(db, c, 'in', 'Qual o valor do ultrassom?', '09:00:00'); msg(db, c, 'out', 'R$ 180,00', '09:02:00');
  const turno = sombra.acharTurnos(db, AGORA)[0];
  assert.ok(/fora do formato/.test((await sombra.avaliarTurno(db, turno, ia(['não sei']))).erro));
  assert.ok(/ponte caiu/.test((await sombra.avaliarTurno(db, turno, ia([new Error('ponte caiu')]))).erro));
  assert.ok(/fora do formato/.test((await sombra.avaliarTurno(db, turno, ia(['{"acao":"inventada","mensagem":""}']))).erro));
});
t('rodada: grava, não repete o mesmo turno e respeita o teto do dia', async () => {
  const db = novoBanco();
  for (let k = 0; k < 5; k++) { const c = contato(db, { tel: '5531888800' + k }); msg(db, c, 'in', 'Qual o valor do ultrassom?', '09:0' + k + ':00'); msg(db, c, 'out', 'R$ 180,00', '09:1' + k + ':00'); }
  const resp = []; for (let k = 0; k < 10; k++) resp.push('{"acao":"responder","mensagem":"R$ 180","motivo":"m"}', '{"categoria":"preco_exame_simples","veredito":"' + (k % 2 ? 'parcial' : 'equivalente') + '","erro_grave":false,"humano_necessario":false,"explicacao":"e"}');
  const f = ia(resp);
  console.log = () => {};
  const r1 = await sombra.rodada(db, { agora: AGORA, forcar: true, chamar: f, n: 3 });
  const r2 = await sombra.rodada(db, { agora: AGORA, forcar: true, chamar: f, n: 3 });
  const r3 = await sombra.rodada(db, { agora: AGORA, forcar: true, chamar: f, n: 3 });
  console.log = logOriginal;
  assert.strictEqual(r1.feitos, 3); assert.strictEqual(r2.feitos, 2); assert.strictEqual(r3.feitos, 0);
  assert.strictEqual(db.prepare('SELECT COUNT(*) AS n FROM sombra_turnos').get().n, 5);
  const antes = sombra.REGRAS.porDia; sombra.REGRAS.porDia = 5;
  assert.strictEqual((await sombra.rodada(db, { agora: AGORA, forcar: true, chamar: f })).pulou, 'teto do dia');
  sombra.REGRAS.porDia = antes;
  // fora do horário, sem forçar, não roda
  assert.ok((await sombra.rodada(db, { agora: D('2026-11-10 22:00:00'), chamar: f })).pulou);
  assert.ok((await sombra.rodada(db, { agora: D('2026-11-08 11:00:00'), chamar: f })).pulou, 'domingo');
});
t('resumo: taxa só sobre comparáveis, erro grave e "pronta para autonomia"', async () => {
  const db = novoBanco();
  const ins = db.prepare("INSERT INTO sombra_turnos (contato_id, in_fim, cliente, real, acao, rascunho, categoria, veredito, erro_grave, erro) VALUES (1, ?, 'c', 'r', 'responder', 'x', ?, ?, ?, ?)");
  let k = 0;
  for (let i = 0; i < 30; i++) ins.run(++k, 'preco_exame_simples', 'equivalente', 0, '');
  ins.run(++k, 'preco_exame_simples', 'nao_comparavel', 0, '');
  for (let i = 0; i < 6; i++) ins.run(++k, 'agenda', 'depende_de_sistema', 0, '');
  ins.run(++k, 'agenda', 'equivalente', 0, ''); ins.run(++k, 'agenda', 'divergente', 0, '');
  ins.run(++k, 'plano', 'parcial', 1, ''); ins.run(++k, '', '', 0, 'ponte caiu');
  const r = sombra.resumo(db, 30);
  assert.strictEqual(r.total, 41); assert.strictEqual(r.com_erro, 1);
  const by = Object.fromEntries(r.categorias.map(c => [c.categoria, c]));
  assert.strictEqual(by.preco_exame_simples.taxa, 100); assert.strictEqual(by.preco_exame_simples.comparaveis, 30); assert.strictEqual(by.preco_exame_simples.pronta, true);
  assert.strictEqual(by.agenda.taxa, 50); assert.strictEqual(by.agenda.depende_de_sistema, 6); assert.strictEqual(by.agenda.pronta, false);
  assert.strictEqual(by.plano.erro_grave, 1);
  assert.strictEqual(r.geral.comparaveis, 33); assert.strictEqual(r.geral.equivalente, 31);
  assert.strictEqual(r.exemplos.length, 2, 'divergente + parcial com erro grave');
});

(async () => {
  for (const [nome, fn] of fila) {
    try { await fn(); console.log('  ok  ', nome); } catch (e) { console.log = logOriginal; falhas++; console.log('  FALHA', nome, '→', e.message); }
  }
  console.log(falhas ? `\n${falhas} FALHA(S)` : '\ntudo ok');
  process.exit(falhas ? 1 : 0);
})();
