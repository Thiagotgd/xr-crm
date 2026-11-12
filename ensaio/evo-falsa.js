'use strict';
// WhatsApp de mentira para o ensaio do CRM: recebe o que o CRM "enviaria", grava num arquivo e responde como a Evolution.
// Nada sai daqui. Uso: node evo-falsa.js [porta] [arquivo-de-registro]
const http = require('http');
const fs = require('fs');
const porta = Number(process.argv[2] || 8197);
const registro = process.argv[3] || '/tmp/evo-falsa.log';
let n = 0;
http.createServer((req, res) => {
  let d = '';
  req.on('data', c => { d += c; });
  req.on('end', () => {
    let j = {};
    try { j = JSON.parse(d || '{}'); } catch (e) {}
    const resumo = JSON.parse(JSON.stringify(j));
    if (resumo.media) resumo.media = '<' + String(resumo.media).length + ' caracteres, começa com "' + String(resumo.media).slice(0, 12) + '">';
    fs.appendFileSync(registro, new Date().toISOString() + ' ' + req.method + ' ' + req.url + ' ' + JSON.stringify(resumo).slice(0, 900) + '\n');
    // texto com a palavra FALHAR simula a Evolution recusando o envio
    if (/FALHAR/.test(String(j.text || j.caption || ''))) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 400, error: 'Bad Request', response: { message: ['simulação de recusa'] } }));
    }
    // igual à Evolution de verdade: arquivo tem de ser endereço (http...) ou base64 puro, sem o cabeçalho "data:...;base64,"
    if (j.media !== undefined && !/^https?:\/\//.test(String(j.media)) && !/^[A-Za-z0-9+/]+={0,2}$/.test(String(j.media))) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 400, error: 'Bad Request', response: { message: ['Owned media must be a url or base64'] } }));
    }
    n++;
    res.writeHead(201, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ key: { id: 'ENSAIO' + Date.now() + n, remoteJid: String(j.number || '') + '@s.whatsapp.net', fromMe: true }, status: 'PENDING' }));
  });
}).listen(porta, '127.0.0.1', () => console.log('evo falsa na porta', porta));
