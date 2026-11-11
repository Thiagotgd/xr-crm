'use strict';
// Testes da retomada de orçamento (retomada.js + o ritmo dentro do followup.avaliar).
// Banco em memória — não toca o crm.db. Uso: node teste-retomada.js (sai 1 se falhar)
const assert = require('assert');
const Database = require('better-sqlite3');
const followup = require('./followup');
const retomada = require('./retomada');

let falhas = 0;
const t = (nome, fn) => { try { fn(); console.log('  ok  ', nome); } catch (e) { falhas++; console.log('  FALHA', nome, '→', e.message); } };
const logOriginal = console.log;
const calado = fn => { console.log = () => {}; try { return fn(); } finally { console.log = logOriginal; } };

function novoBanco() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE contatos (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT DEFAULT '', telefone TEXT DEFAULT '', pet_nome TEXT DEFAULT '', etapa TEXT DEFAULT 'novo',
      created_at TEXT DEFAULT (datetime('now','localtime')), updated_at TEXT DEFAULT (datetime('now','localtime')), orcamento_enviado_at TEXT DEFAULT '');
    CREATE TABLE mensagens (id INTEGER PRIMARY KEY AUTOINCREMENT, contato_id INTEGER, direcao TEXT, texto TEXT, tipo TEXT DEFAULT 'text',
      created_at TEXT DEFAULT (datetime('now','localtime')));
    CREATE TABLE etapa_historico (id INTEGER PRIMARY KEY AUTOINCREMENT, contato_id INTEGER, etapa_de TEXT, etapa_para TEXT, created_at TEXT DEFAULT (datetime('now','localtime')));
  `);
  followup.initFollowup(db);
  return db;
}
const p2 = n => String(n).padStart(2, '0');
const sql = d => `${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())} ${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}`;
const D = (s) => new Date(s.replace(' ', 'T'));

console.log('nomes');
t('primeiro nome simples', () => assert.strictEqual(retomada.primeiroNome('maria silva'), 'Maria'));
t('nome com acento', () => assert.strictEqual(retomada.primeiroNome('Mônica'), 'Mônica'));
t('clínica não é nome', () => assert.strictEqual(retomada.primeiroNome('Clínica Vet Vida'), ''));
t('"Dra" não é nome', () => assert.strictEqual(retomada.primeiroNome('Dra Paula'), ''));
t('emoji/apelido estranho → sem nome', () => assert.strictEqual(retomada.primeiroNome('🐶Ju'), ''));
t('vazio → sem nome', () => assert.strictEqual(retomada.primeiroNome(''), ''));
t('pet "Animal" (nome genérico do orçamento) é ignorado', () => assert.strictEqual(retomada.nomePet('Animal'), ''));
t('pet normal', () => assert.strictEqual(retomada.nomePet('luna'), 'Luna'));

console.log('texto sugerido');
{
  const db = novoBanco();
  const add = (o) => db.prepare("INSERT INTO contatos (nome, telefone, pet_nome, etapa, orcamento_enviado_at, orcamento_modalidade, followup_nivel) VALUES (?,?,?,?,?,?,?)")
    .run(o.nome || '', o.tel || '553188887777', o.pet || '', 'orcamento', o.orc, o.mod || 'TC', 2).lastInsertRowid;
  const msg = (id, dir, texto, quando, tipo) => db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, tipo, created_at) VALUES (?,?,?,?,?)").run(id, dir, texto, tipo || 'text', quando);
  const get = id => db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
  const manha = D('2026-11-10 09:30:00'), tarde = D('2026-11-10 15:00:00');

  const a = add({ nome: 'Carla Souza', pet: 'Luna', orc: '2026-11-09 10:00:00', mod: 'TC' });
  msg(a, 'out', 'Orçamento Tomografia - Luna (Carla).pdf', '2026-11-09 10:00:00', 'document');
  t('1ª mensagem padrão: nome, pet, exame, horário a preencher, pré-anestésicos', () => {
    const s = retomada.sugerir(db, get(a), manha);
    assert.strictEqual(s.etapa, 1); assert.strictEqual(s.rotulo, '1ª mensagem');
    assert.ok(s.texto.startsWith('Bom dia, Carla! Tudo bem?'), s.texto);
    assert.ok(/orçamento da tomografia da Luna/.test(s.texto), s.texto);
    assert.ok(s.texto.includes('[dia e hora]') && s.preencher === true);
    assert.ok(/pré-anestésicos aqui, no mesmo dia/.test(s.texto));
  });
  t('saudação muda com a hora', () => assert.ok(retomada.sugerir(db, get(a), tarde).texto.startsWith('Boa tarde, Carla!')));

  const b = add({ nome: 'Pet Shop Amigo', pet: '', orc: '2026-11-09 10:00:00', mod: 'US', tel: '553799990000' });
  t('sem nome confiável, sem pet, ultrassom (sem anestesia), de fora de BH', () => {
    const s = retomada.sugerir(db, get(b), manha);
    assert.ok(s.texto.startsWith('Bom dia! Tudo bem?'), s.texto);
    assert.ok(/orçamento do ultrassom do seu pet/.test(s.texto), s.texto);
    assert.ok(!/pré-anestésicos/.test(s.texto));
    assert.ok(/uma viagem só/.test(s.texto));
  });

  const c = add({ nome: 'João', pet: 'Thor', orc: '2026-11-09 10:00:00', mod: 'RM' });
  msg(c, 'in', 'Vou fazer os exames de sangue amanhã e te mando', '2026-11-09 10:30:00');
  msg(c, 'out', 'Combinado, fico no aguardo', '2026-11-09 10:31:00');
  t('cliente aguardando exames → pergunta dos exames, sem colchete', () => {
    const s = retomada.sugerir(db, get(c), manha);
    assert.strictEqual(s.motivo, 'aguardando exames');
    assert.ok(/Os exames do Thor já ficaram prontos\?/.test(s.texto), s.texto);
    assert.ok(/deixo a ressonância agendada/.test(s.texto));
    assert.strictEqual(s.preencher, false);
  });

  const d = add({ nome: 'Ana', pet: 'Mel', orc: '2026-11-09 10:00:00', mod: 'RM' });
  msg(d, 'out', 'Verifiquei no plano e a Ressonância não está liberada', '2026-11-09 10:20:00');
  t('plano não liberou → condição no particular', () => {
    const s = retomada.sugerir(db, get(d), manha);
    assert.strictEqual(s.motivo, 'plano não liberou');
    assert.ok(/12x sem juros/.test(s.texto) && /\[dia e hora\]/.test(s.texto));
  });

  const e = add({ nome: 'Rita', pet: 'Bob', orc: '2026-11-06 10:00:00', mod: 'TC' });
  msg(e, 'in', 'achei caro, tem desconto?', '2026-11-06 10:10:00');
  msg(e, 'out', 'Conseguimos parcelar', '2026-11-06 10:12:00');
  msg(e, 'in', 'ok obrigada', '2026-11-06 10:13:00');
  db.prepare("INSERT INTO followup_eventos (contato_id, tipo, valor, created_at) VALUES (?, 'retomada', '1', '2026-11-07 09:00:00')").run(e);
  t('2ª mensagem para quem falou de valor', () => {
    const s = retomada.sugerir(db, get(e), manha);
    assert.strictEqual(s.etapa, 2); assert.strictEqual(s.feitas, 1); assert.strictEqual(s.rotulo, '2ª mensagem');
    assert.ok(/Como está o Bob\?/.test(s.texto) && /Sobre o valor da tomografia/.test(s.texto), s.texto);
  });
  db.prepare("INSERT INTO followup_eventos (contato_id, tipo, valor, created_at) VALUES (?, 'retomada', '2', '2026-11-09 09:00:00')").run(e);
  t('última mensagem: encerra e segura condições', () => {
    const s = retomada.sugerir(db, get(e), manha);
    assert.strictEqual(s.etapa, 3); assert.strictEqual(s.rotulo, 'Última mensagem');
    assert.ok(/Vou encerrar por aqui o orçamento da tomografia do Bob/.test(s.texto) && /15 dias/.test(s.texto), s.texto);
    assert.strictEqual(s.preencher, false);
  });

  console.log('ciclo');
  t('"ok obrigada" depois da retomada NÃO reinicia a contagem', () => {
    msg(e, 'in', 'ok, obrigada', '2026-11-09 09:30:00');
    assert.strictEqual(retomada.estado(db, get(e), manha).feitas, 2);
  });
  t('mensagem de verdade do cliente reinicia o ciclo', () => {
    msg(e, 'in', 'Oi, ainda tem horário para quinta? Quero agendar a tomografia', '2026-11-09 16:00:00');
    assert.strictEqual(retomada.estado(db, get(e), manha).feitas, 0);
    assert.strictEqual(retomada.sugerir(db, get(e), manha).etapa, 1);
  });
}

console.log('registro automático (webhook)');
{
  const db = novoBanco();
  const id = db.prepare("INSERT INTO contatos (nome, telefone, etapa, orcamento_enviado_at, orcamento_modalidade, followup_nivel) VALUES ('Lia','553188880000','orcamento', datetime('now','localtime','-2 days'), 'TC', 2)").run().lastInsertRowid;
  const reg = (txt, tipo) => calado(() => retomada.registrarSeRetomada(db, id, txt, tipo || 'text'));
  t('mensagem da Athena não conta', () => assert.strictEqual(reg('🐾 _Assistente virtual Athena_ 🐾 Olá'), null));
  t('mensagem de agendamento não é retomada (inclui plural)', () => { assert.strictEqual(reg('Exames agendados - Data: quinta'), null); assert.strictEqual(reg('Exame agendado para amanhã'), null); });
  t('orçamento novo em PDF não é retomada', () => assert.strictEqual(reg('Orçamento Tomografia - Mel (Lia).pdf', 'document'), null));
  t('1ª mensagem para contato 🟡 registra etapa 1', () => assert.strictEqual(reg('Bom dia, Lia! Ficou alguma dúvida?'), 1));
  t('linhas seguintes na mesma hora não contam de novo', () => assert.strictEqual(reg('Tenho horário amanhã às 11h'), null));
  t('contato fora da fila (nível 0) não registra', () => {
    const o = db.prepare("INSERT INTO contatos (nome, telefone, etapa, orcamento_enviado_at, followup_nivel) VALUES ('Zé','553188881111','orcamento', datetime('now','localtime','-1 hours'), 0)").run().lastInsertRowid;
    assert.strictEqual(calado(() => retomada.registrarSeRetomada(db, o, 'Oi!', 'text')), null);
  });
  t('contato agendado não registra', () => {
    const o = db.prepare("INSERT INTO contatos (nome, telefone, etapa, orcamento_enviado_at, followup_nivel) VALUES ('Bia','553188882222','agendado', datetime('now','localtime','-3 days'), 2)").run().lastInsertRowid;
    assert.strictEqual(calado(() => retomada.registrarSeRetomada(db, o, 'Oi!', 'text')), null);
  });
  t('resumo(): 1 contato retomado, sem resposta ainda', () => {
    const r = retomada.resumo(db, '2000-01-01 00:00:00', () => null);
    assert.strictEqual(r.contatos, 1); assert.strictEqual(r.mensagens, 1); assert.strictEqual(r.responderam, 0); assert.strictEqual(r.por_etapa[1], 1);
  });
  t('resumo(): cliente respondeu e agendou depois', () => {
    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, created_at) VALUES (?, 'in', 'pode agendar', datetime('now','localtime','+1 minutes'))").run(id);
    const r = retomada.resumo(db, '2000-01-01 00:00:00', (cid) => cid === id ? 'x' : null);
    assert.strictEqual(r.responderam, 1); assert.strictEqual(r.agendaram, 1); assert.strictEqual(r.taxa_agendou, 100);
  });
}

console.log('ritmo dentro do follow-up (dia seguinte → 3º dia → 7º dia → arquiva)');
{
  const db = novoBanco();
  // Orçamento numa segunda 10h; a recepção falou por último.
  const id = db.prepare("INSERT INTO contatos (nome, telefone, pet_nome, etapa, orcamento_enviado_at, orcamento_modalidade) VALUES ('Paula','553188883333','Nina','orcamento','2026-11-16 10:00:00','TC')").run().lastInsertRowid;
  const msg = (dir, texto, quando) => db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, created_at) VALUES (?,?,?,?)").run(id, dir, texto, quando);
  const ev = (etapa, quando) => db.prepare("INSERT INTO followup_eventos (contato_id, tipo, valor, created_at) VALUES (?, 'retomada', ?, ?)").run(id, String(etapa), quando);
  const c = () => db.prepare('SELECT * FROM contatos WHERE id = ?').get(id);
  const roda = quando => calado(() => followup.avaliar(db, D(quando)));
  msg('out', 'Segue o orçamento', '2026-11-16 10:00:00');

  t('segunda à tarde: ainda dentro do prazo', () => { roda('2026-11-16 16:00:00'); assert.strictEqual(c().followup_nivel, 0); });
  t('terça de manhã: 🟡 com a 1ª mensagem', () => {
    roda('2026-11-17 09:00:00'); assert.strictEqual(c().followup_nivel, 2);
    assert.strictEqual(retomada.sugerir(db, c(), D('2026-11-17 09:00:00')).etapa, 1);
  });
  t('recepção envia a 1ª: sai da fila', () => {
    ev(1, '2026-11-17 09:10:00'); msg('out', 'Bom dia, Paula! Ficou alguma dúvida?', '2026-11-17 09:10:00');
    roda('2026-11-17 10:00:00'); assert.strictEqual(c().followup_nivel, 0);
  });
  t('quarta: continua fora da fila (2ª só no 3º dia)', () => { roda('2026-11-18 12:00:00'); assert.strictEqual(c().followup_nivel, 0); });
  t('quinta: volta em ⚪ pedindo a 2ª mensagem', () => {
    roda('2026-11-19 12:00:00'); assert.strictEqual(c().followup_nivel, 3);
    assert.ok(/2ª mensagem/.test(c().followup_motivo), c().followup_motivo);
    assert.strictEqual(retomada.sugerir(db, c(), D('2026-11-19 12:00:00')).etapa, 2);
  });
  t('recepção envia a 2ª: some até o 7º dia', () => {
    ev(2, '2026-11-19 12:10:00'); msg('out', 'Como está a Nina?', '2026-11-19 12:10:00');
    roda('2026-11-21 12:00:00'); assert.strictEqual(c().followup_nivel, 0);
  });
  t('segunda seguinte: ⚪ com a última mensagem', () => {
    roda('2026-11-23 13:00:00'); assert.strictEqual(c().followup_nivel, 3);
    assert.ok(/última mensagem/.test(c().followup_motivo), c().followup_motivo);
    assert.strictEqual(retomada.sugerir(db, c(), D('2026-11-23 13:00:00')).etapa, 3);
  });
  t('recepção envia a última: fica em Orçamento esperando resposta', () => {
    ev(3, '2026-11-23 13:10:00'); msg('out', 'Vou encerrar por aqui', '2026-11-23 13:10:00');
    roda('2026-11-24 13:00:00'); assert.strictEqual(c().followup_nivel, 0); assert.strictEqual(c().etapa, 'orcamento');
  });
  t('2 dias sem resposta à última: arquiva em Finalizado', () => {
    roda('2026-11-25 14:00:00'); assert.strictEqual(c().etapa, 'finalizado');
    const e = db.prepare("SELECT valor FROM followup_eventos WHERE contato_id = ? AND tipo = 'encerrado' ORDER BY id DESC LIMIT 1").get(id);
    assert.ok(/3 retomadas/.test(e.valor), e.valor);
  });

  // Sem nenhuma retomada registrada, a régua antiga continua valendo.
  const id2 = db.prepare("INSERT INTO contatos (nome, telefone, etapa, orcamento_enviado_at) VALUES ('Sem Retomada','553188884444','orcamento','2026-11-16 10:00:00')").run().lastInsertRowid;
  db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, created_at) VALUES (?, 'out', 'Segue o orçamento', '2026-11-16 10:00:00')").run(id2);
  const c2 = () => db.prepare('SELECT * FROM contatos WHERE id = ?').get(id2);
  t('sem retomada: 🟡 no dia seguinte e ⚪ depois de 48h (régua antiga)', () => {
    roda('2026-11-17 09:00:00'); assert.strictEqual(c2().followup_nivel, 2);
    roda('2026-11-19 09:00:00'); assert.strictEqual(c2().followup_nivel, 3);
    assert.strictEqual(retomada.sugerir(db, c2(), D('2026-11-19 09:00:00')).etapa, 1, 'em ⚪ sem retomada feita, ainda é a 1ª mensagem');
  });
  t('cliente responde de verdade: vira 🔴 e o ciclo zera', () => {
    db.prepare("INSERT INTO mensagens (contato_id, direcao, texto, created_at) VALUES (?, 'in', 'Qual o horário disponível amanhã?', '2026-11-19 10:00:00')").run(id2);
    roda('2026-11-19 11:00:00'); assert.strictEqual(c2().followup_nivel, 1);
  });
}

console.log(falhas ? `\n${falhas} FALHA(S)` : '\ntudo ok');
process.exit(falhas ? 1 : 0);
