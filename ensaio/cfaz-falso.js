'use strict';
// CFAZ de mentira para o ensaio do CRM: responde agendas, horários livres e marcações, e aceita criar e apagar.
// Nada vai ao CFAZ de verdade. Uso: node cfaz-falso.js [porta] [arquivo-de-registro]
const http = require('http');
const fs = require('fs');
const porta = Number(process.argv[2] || 8198);
const registro = process.argv[3] || '/tmp/cfaz-falso.log';
const agendas = [
  { id: 2713, name: 'Tomografia ', enabled: true, default_event_duration: 20 },
  { id: 2731, name: 'Ressonância Magnética', enabled: true, default_event_duration: 20 },
  { id: 4931, name: 'Cardiologistas', enabled: true, default_event_duration: 20 },
  { id: 2723, name: 'Maria Luiza', enabled: true, default_event_duration: 20 },
  { id: 2032, name: 'Lucas', enabled: false, default_event_duration: 20 },
];
let seq = 900000;
let eventos = [
  { id: ++seq, calendar_id: 2713, title: 'TC', start_date: '2026-11-14T10:00:00.000-03:00', end_date: '2026-11-14T10:20:00.000-03:00', status: 0, phone: '+55 31900000000' },
  { id: ++seq, calendar_id: 2713, title: 'TC + ECG', start_date: '2026-11-14T11:00:00.000-03:00', end_date: '2026-11-14T11:20:00.000-03:00', status: 0, phone: '' },
];
const manda = (res, st, j) => { res.writeHead(st, { 'Content-Type': 'application/json' }); res.end(j === null ? '' : JSON.stringify(j)); };
http.createServer((req, res) => {
  let d = '';
  req.on('data', c => { d += c; });
  req.on('end', () => {
    const u = new URL(req.url, 'http://x');
    let corpo = {};
    try { corpo = JSON.parse(d || '{}'); } catch (e) {}
    if (req.method !== 'GET') fs.appendFileSync(registro, new Date().toISOString() + ' ' + req.method + ' ' + req.url + ' ' + JSON.stringify(corpo).slice(0, 600) + '\n');
    if (req.headers.authorization !== 'Token token=ensaio') return manda(res, 401, { error: 'sem credencial' });
    let m;
    if (req.method === 'GET' && u.pathname === '/api/v1/calendars.json') return manda(res, 200, agendas);
    if (req.method === 'GET' && (m = u.pathname.match(/^\/api\/v1\/calendars\/(\d+)\/available_slots\.json$/))) {
      const cal = Number(m[1]), data = u.searchParams.get('date');
      const ocup = new Set(eventos.filter(e => e.calendar_id === cal && e.start_date.startsWith(data)).map(e => e.start_date.slice(11, 16)));
      const livres = [];
      for (let h = 8; h < 18; h++) for (const mm of ['00', '20', '40']) { const hh = String(h).padStart(2, '0') + ':' + mm; if (!ocup.has(hh)) livres.push({ text_option: hh, start_hour: hh }); }
      return manda(res, 200, { available_slots: livres, calendar_id: cal, date: data });
    }
    if (req.method === 'GET' && u.pathname === '/api/v1/events.json') return manda(res, 200, { data: u.searchParams.get('page') === '1' ? eventos : [], pagination: {} });
    if (req.method === 'POST' && u.pathname === '/api/v1/events.json') {
      const e = corpo.event || {};
      const erros = [];
      const ag = agendas.find(a => a.id === e.calendar_id);
      if (e.status === undefined || e.status === null) erros.push('Status não pode ficar em branco');
      if (!ag) erros.push('Agenda não encontrada'); else if (!ag.enabled) erros.push('Não é possível criar um agendamento em uma agenda desabilitada.');
      if (!e.title) erros.push('Título não pode ficar em branco');
      if (eventos.some(x => x.calendar_id === e.calendar_id && x.start_date === e.start_date)) erros.push('Já existe um agendamento nesse horário');
      if (erros.length) return manda(res, 422, { errors: erros });
      const novo = Object.assign({ id: ++seq }, e);
      eventos.push(novo);
      return manda(res, 200, novo);
    }
    if (req.method === 'DELETE' && (m = u.pathname.match(/^\/api\/v1\/events\/(\d+)\.json$/))) {
      const antes = eventos.length;
      eventos = eventos.filter(e => e.id !== Number(m[1]));
      return manda(res, eventos.length < antes ? 200 : 404, null);
    }
    return manda(res, 404, { error: 'Ops! Não encontramos essa página' });
  });
}).listen(porta, '127.0.0.1', () => console.log('cfaz falso na porta', porta));
