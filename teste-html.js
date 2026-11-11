'use strict';
// Checa a sintaxe de TODOS os <script> do index.html e acusa função declarada duas vezes
// (a última vence por hoisting e rouba o clique — já aconteceu com loadMetricas em 21/08/2026).
// OBRIGATÓRIO antes de subir: um erro de JS derruba a página inteira.
// Uso: node teste-html.js [caminho/index.html]
const fs = require('fs');
const vm = require('vm');

const arq = process.argv[2] || require('path').join(__dirname, 'index.html');
const html = fs.readFileSync(arq, 'utf8');
const scripts = [...html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)].map(m => m[1]);
if (!scripts.length) { console.log('nenhum <script> inline encontrado em', arq); process.exit(1); }

let erro = 0;
scripts.forEach((src, i) => {
  try { new vm.Script(src, { filename: `index.html#script${i + 1}` }); console.log(`  ok   script ${i + 1} (${src.length} chars)`); }
  catch (e) { erro++; console.log(`  ERRO script ${i + 1}: ${e.message}`); const m = /:(\d+)/.exec(e.stack || ''); if (m) console.log('    linha dentro do script:', m[1]); }
});

// Funções declaradas mais de uma vez (top-level ou não — qualquer duplicata é suspeita)
const nomes = {};
for (const src of scripts) for (const m of src.matchAll(/^\s*(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(/gm)) nomes[m[1]] = (nomes[m[1]] || 0) + 1;
const dup = Object.entries(nomes).filter(([, n]) => n > 1);
if (dup.length) { erro++; console.log('  ERRO função duplicada:', dup.map(([k, n]) => `${k} x${n}`).join(', ')); }
else console.log(`  ok   ${Object.keys(nomes).length} funções, nenhuma duplicada`);

// Chamadas a funções de origem que precisam existir
for (const f of ['origemTag', 'renderFollowup', 'renderMetricasVendas', 'loadMetricasVendas', 'fuRetBloco', 'fuRetInput', 'fuRetEnviar', 'sombraCard', 'leituraBloco', 'leitorCard']) {
  if (!nomes[f]) { erro++; console.log('  ERRO função esperada não encontrada:', f); }
}

console.log(erro ? `\n${erro} problema(s)` : '\nhtml ok');
process.exit(erro ? 1 : 0);
