'use strict';
// Ensaio manual do leitor de anexos com arquivos reais, SEM gravar nada (banco aberto só para leitura).
// Uso (dentro do container xr-apps, na pasta do xr-crm):
//   node leitor-cli.js          → lê 3 anexos pendentes e imprime o que entendeu
//   node leitor-cli.js 6        → lê 6
//   node leitor-cli.js --contar → só conta os pendentes das últimas 36 h
const path = require('path');
const Database = require('better-sqlite3');
const leitor = require('./leitor');

(async () => {
  const arg = process.argv.slice(2);
  const db = new Database(process.env.CRM_DB || path.join(__dirname, 'crm.db'), { readonly: true });
  const pend = leitor.pendentes(db, new Date(), 500);
  const porNome = pend.filter(m => leitor.tipoPeloNome(m.tipo === 'document' ? m.texto : ''));
  console.log(`pendentes nas últimas ${leitor.REGRAS.janelaHoras} h: ${pend.length} (${porNome.length} resolvidos pelo nome, ${pend.length - porNome.length} para a IA)`);
  if (arg.includes('--contar')) return;
  const n = Number(arg.find(a => /^\d+$/.test(a))) || 3;
  const fake = { prepare: s => db.prepare(s) };   // lerMensagem grava; aqui chamamos a ponte direto para não escrever
  const { SYSTEM, REGRAS, lerJson, detalhe } = leitor;
  const fs = require('fs');
  const token = fs.readFileSync(path.join(__dirname, 'secrets', 'claude-bridge.key'), 'utf8').trim();
  for (const m of pend.filter(x => !leitor.tipoPeloNome(x.tipo === 'document' ? x.texto : '')).slice(0, n)) {
    const t0 = Date.now();
    const r = await fetch('http://172.18.0.1:18802/arquivo', { method: 'POST', headers: { 'content-type': 'application/json', 'x-bridge-token': token },
      body: JSON.stringify({ prompt: 'Leia o anexo e devolva o JSON.', system: SYSTEM, model: REGRAS.modelo, arquivo: m.media_path, raiz: 'xr-crm', origem: 'xr-crm-leitor-ensaio' }) });
    const j = await r.json().catch(() => null);
    console.log('\n────────────────────────────────────────');
    console.log(`msg ${m.id} · ${m.tipo} · etapa ${m.etapa} · ${Date.now() - t0} ms`);
    if (!r.ok || !j || !j.ok) { console.log('ERRO:', r.status, j && j.error); continue; }
    const d = lerJson(j.result);
    if (!d) { console.log('fora do formato:', j.result.slice(0, 200)); continue; }
    const det = detalhe({ tipo: d.tipo, dados: JSON.stringify(d), resumo: d.resumo, confianca: d.confianca, origem: 'ia', erro: '' });
    console.log('TIPO     :', d.tipo, '· confiança', d.confianca || '-');
    console.log('RESUMO   :', d.resumo);
    if (det && det.exames.length) console.log('EXAMES   :', det.exames.join(' | '));
    if (det && det.paciente) console.log('PACIENTE :', det.paciente);
    if (det && det.suspeita) console.log('SUSPEITA :', det.suspeita);
    if (det && det.ilegivel.length) console.log('ILEGÍVEL :', det.ilegivel.join(' | '));
  }
  void fake;
  db.close();
  process.exit(0);   // encerra de vez (conexões abertas com a ponte seguravam o processo)
})().catch(e => { console.error(e); process.exit(1); });
