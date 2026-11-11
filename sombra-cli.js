'use strict';
// Ensaio manual do modo sombra com conversas reais, SEM gravar nada (banco aberto só para leitura).
// Uso (dentro do container xr-apps, na pasta do xr-crm):
//   node sombra-cli.js            → avalia 3 turnos sorteados das últimas 30 horas e imprime
//   node sombra-cli.js 5 48       → 5 turnos das últimas 48 horas
//   node sombra-cli.js --contar   → só conta quantos turnos existem na janela (não chama a IA)
const path = require('path');
const Database = require('better-sqlite3');
const sombra = require('./sombra');

(async () => {
  const arg = process.argv.slice(2);
  const banco = process.env.CRM_DB || path.join(__dirname, 'crm.db');
  const db = new Database(banco, { readonly: true });
  const horas = Number(arg.find((a, i) => i === 1 && /^\d+$/.test(a))) || 30;
  const turnos = sombra.acharTurnos(db, new Date(), { janelaHoras: horas });
  if (arg.includes('--contar')) { console.log(`turnos comparáveis nas últimas ${horas} h: ${turnos.length}`); return; }
  const n = Number(arg.find((a, i) => i === 0 && /^\d+$/.test(a))) || 3;
  for (let i = turnos.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [turnos[i], turnos[j]] = [turnos[j], turnos[i]]; }
  console.log(`turnos na janela: ${turnos.length}; avaliando ${Math.min(n, turnos.length)}`);
  for (const t of turnos.slice(0, n)) {
    const l = await sombra.avaliarTurno(db, t);
    console.log('\n────────────────────────────────────────');
    console.log('CLIENTE :', t.cliente.replace(/\n/g, ' / ').slice(0, 400));
    console.log('REAL    :', t.real.replace(/\n/g, ' / ').slice(0, 500));
    if (l.erro) { console.log('ERRO    :', l.erro); continue; }
    console.log('SOMBRA  : [' + l.acao + ']', l.rascunho.replace(/\n/g, ' / ').slice(0, 500));
    console.log('MOTIVO  :', l.motivo);
    console.log('VEREDITO:', l.veredito, '·', l.categoria, l.erro_grave ? '· ERRO GRAVE' : '', l.humano_necessario ? '· precisava de humano' : '');
    console.log('PORQUÊ  :', l.explicacao, `(${l.ms} ms; ${l.modelo_rascunho} / ${l.modelo_juiz})`);
  }
})().catch(e => { console.error(e); process.exit(1); });
