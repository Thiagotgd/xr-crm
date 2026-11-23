'use strict';
// Aplica (ou só simula) a regra de "lido depois da sincronização" numa janela, direto no crm.db.
// Uso, na pasta do xr-crm:  node conexao-lidos-cli.js "2026-11-19 18:00:00" "2026-11-23 09:33:21" [--aplicar]
// Sem --aplicar só mostra o que faria. Roda com o CRM no ar (SQLite aguenta), mas de preferência fora de pico.
const path = require('path');
const Database = require('better-sqlite3');
const followup = require('./followup');
const conexao = require('./conexao');

const [desde, ate] = process.argv.slice(2, 4);
const aplicar = process.argv.includes('--aplicar');
if (!desde || !ate) { console.error('uso: node conexao-lidos-cli.js "AAAA-MM-DD HH:MM:SS" "AAAA-MM-DD HH:MM:SS" [--aplicar]'); process.exit(1); }

const db = new Database(path.join(__dirname, 'crm.db'));
conexao.iniciar({ db, evoBase: 'http://127.0.0.1:1', evoKey: '', instancia: 'cli', processarMensagem: null, marcas: [followup.ATHENA_MARCA, followup.SAUDACAO_MARCA] });
const r = conexao.aplicarLidos({ desde, ate, simular: !aplicar });
console.log(`${aplicar ? 'APLICADO' : 'SIMULAÇÃO'}: ${r.conversas} conversas com mensagem do cliente na janela · ${r.marcadas} ficam lidas · ${r.mantidas} continuam não lidas · ${r.semNaoLida} já estavam lidas`);
for (const d of r.detalhes) console.log(' ', d.acao === 'lida' ? '✓' : '•', d.contato, '|', d.acao, '|', d.motivo || (d.pendentes + ' pendente(s), última ' + d.ultima + ': ' + JSON.stringify(d.texto)));
