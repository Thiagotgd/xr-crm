'use strict';
// Testes do módulo origem.js — roda com banco em memória (não toca o crm.db).
// Uso: node teste-origem.js   (sai com código 1 se algo falhar)
const assert = require('assert');
const Database = require('better-sqlite3');
const origem = require('./origem');

let falhas = 0;
const t = (nome, fn) => { try { fn(); console.log('  ok  ', nome); } catch (e) { falhas++; console.log('  FALHA', nome, '→', e.message); } };

console.log('detectar()');
t('texto pronto do anúncio Meta', () => assert.strictEqual(origem.detectar('Olá! Posso ter mais informações sobre isso?'), 'meta'));
t('texto pronto sem acento/maiúsculas', () => assert.strictEqual(origem.detectar('ola posso ter mais informacoes sobre isso'), 'meta'));
t('quick reply do anúncio', () => assert.strictEqual(origem.detectar('Gostaria de obter mais informações'), 'meta'));
t('frase futura por anúncio', () => assert.strictEqual(origem.detectar('Oi, vi o anúncio sobre ressonância para cães'), 'meta'));
t('veio pelo Instagram', () => assert.strictEqual(origem.detectar('Boa tarde, vim pelo Instagram'), 'meta'));
t('vim do Google', () => assert.strictEqual(origem.detectar('Olá, vim do Google e gostaria de mais informações por favor!'), 'google'));
t('achei vocês no Google', () => assert.strictEqual(origem.detectar('achei vcs no google, fazem tomografia?'), 'google'));
t('bom dia comum → null', () => assert.strictEqual(origem.detectar('Bom dia, tudo bem?'), null));
t('pergunta de preço → null', () => assert.strictEqual(origem.detectar('qual o valor da ressonância?'), null));
t('"google" solto no meio não é origem', () => assert.strictEqual(origem.detectar('mandei o laudo pelo google drive'), null));
t('vazio → null', () => assert.strictEqual(origem.detectar(''), null));

console.log('classificar() / backfill() / resumo()');
const db = new Database(':memory:');
db.exec(`
  CREATE TABLE contatos (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT, telefone TEXT, etapa TEXT DEFAULT 'novo',
    created_at TEXT DEFAULT (datetime('now','localtime')), orcamento_enviado_at TEXT DEFAULT '', followup_ignorar INTEGER DEFAULT 0);
  CREATE TABLE mensagens (id INTEGER PRIMARY KEY AUTOINCREMENT, contato_id INTEGER, direcao TEXT, texto TEXT, tipo TEXT DEFAULT 'text',
    created_at TEXT DEFAULT (datetime('now','localtime')));
  CREATE TABLE followup_eventos (id INTEGER PRIMARY KEY AUTOINCREMENT, contato_id INTEGER, tipo TEXT, valor TEXT, created_at TEXT DEFAULT (datetime('now','localtime')));
`);
origem.init(db);
origem.init(db); // idempotente
const novo = tel => db.prepare("INSERT INTO contatos (nome, telefone) VALUES ('', ?)").run(tel).lastInsertRowid;
const msg = (id, dir, texto) => db.prepare("INSERT INTO mensagens (contato_id, direcao, texto) VALUES (?, ?, ?)").run(id, dir, texto);
const orig = id => db.prepare('SELECT origem, origem_detalhe FROM contatos WHERE id = ?').get(id);

t('1ª msg = texto do anúncio → meta com detalhe', () => {
  const id = novo('5531900000001'); msg(id, 'in', 'Olá! Posso ter mais informações sobre isso?');
  assert.strictEqual(origem.classificar(db, id, 'Olá! Posso ter mais informações sobre isso?'), 'meta');
  assert.strictEqual(orig(id).origem_detalhe, 'Olá! Posso ter mais informações sobre isso?');
});
t('1ª msg "bom dia", 2ª "vim do google" → google (janela de 3)', () => {
  const id = novo('5531900000002');
  msg(id, 'in', 'Bom dia'); assert.strictEqual(origem.classificar(db, id, 'Bom dia'), 'direto');
  msg(id, 'in', 'vim do Google, fazem RM?'); assert.strictEqual(origem.classificar(db, id, 'vim do Google, fazem RM?'), 'google');
});
t('depois da janela, "vim do google" não muda mais', () => {
  const id = novo('5531900000003');
  for (const x of ['oi', 'tudo bem?', 'quanto custa?', 'ok']) { msg(id, 'in', x); origem.classificar(db, id, x); }
  msg(id, 'in', 'ah, vim do google'); origem.classificar(db, id, 'ah, vim do google');
  assert.strictEqual(orig(id).origem, 'direto');
});
t('meta não vira google depois', () => {
  const id = novo('5531900000004');
  msg(id, 'in', 'Olá! Posso ter mais informações sobre isso?'); origem.classificar(db, id, 'Olá! Posso ter mais informações sobre isso?');
  msg(id, 'in', 'vim do google'); origem.classificar(db, id, 'vim do google');
  assert.strictEqual(orig(id).origem, 'meta');
});
t('backfill só mexe em quem está sem origem', () => {
  const a = novo('5531900000005'); msg(a, 'in', 'Boa tarde'); msg(a, 'in', 'Olá, vim do Google e gostaria de mais informações');
  const b = novo('5531900000006'); msg(b, 'in', 'Oi'); msg(b, 'out', 'Olá!'); msg(b, 'in', 'valor do US?');
  const c = novo('5531900000007'); // sem mensagem nenhuma
  const n = origem.backfill(db);
  assert.strictEqual(n, 3);
  assert.strictEqual(orig(a).origem, 'google');
  assert.strictEqual(orig(b).origem, 'direto');
  assert.strictEqual(orig(c).origem, 'direto');
  assert.strictEqual(origem.backfill(db), 0);
});
t('resumo() conta contatos, conversas e agendados por origem', () => {
  // contato 1 (meta) agendou pela mensagem do time; contato 2 (google) só conversou; contato 4 (meta) marcou por comando
  msg(1, 'out', 'Exame agendado para quinta às 9h');
  db.prepare("INSERT INTO followup_eventos (contato_id, tipo, valor) VALUES (4, 'comando', 'marcou')").run();
  db.prepare("UPDATE contatos SET orcamento_enviado_at = datetime('now','localtime') WHERE id IN (1,2)").run();
  const r = origem.resumo(db, 30);
  const by = Object.fromEntries(r.origens.map(o => [o.origem, o]));
  assert.strictEqual(r.total, 7);
  assert.strictEqual(by.meta.contatos, 2); assert.strictEqual(by.meta.agendados, 2); assert.strictEqual(by.meta.orcados, 1);
  assert.strictEqual(by.google.contatos, 2); assert.strictEqual(by.google.agendados, 0); assert.strictEqual(by.google.conversaram, 2);
  assert.strictEqual(by.direto.contatos, 3);
  assert.strictEqual(by.meta.taxa, 100);
  assert.ok(r.detalhes.some(d => d.origem === 'meta' && d.contatos === 2 && d.agendados === 2), 'detalhe por frase do anúncio');
  assert.strictEqual(r.origens[r.origens.length - 1].origem, 'direto', 'direto por último');
});
t('resumo() ignora a Athena como "agendou"', () => {
  const id = novo('5531900000008'); msg(id, 'in', 'oi'); origem.classificar(db, id, 'oi');
  msg(id, 'out', '🐾 _Assistente virtual Athena_ 🐾 Seu exame agendado para amanhã'); // texto da Athena não conta
  const r = origem.resumo(db, 30, 'Assistente virtual Athena');
  assert.strictEqual(r.origens.find(o => o.origem === 'direto').agendados, 0);
});

console.log(falhas ? `\n${falhas} FALHA(S)` : '\ntudo ok');
process.exit(falhas ? 1 : 0);
