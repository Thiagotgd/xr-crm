'use strict';
// ─── Identificador novo do WhatsApp (@lid) → telefone (23/11/2026) ─────────────────────────────────────────────
// O WhatsApp passou a identificar contatos por um número interno ("LID", ex.: 74032368082963@lid) em vez do
// telefone. Nas mensagens ao vivo a Evolution já traduz (key.remoteJid = telefone, key.remoteJidAlt). No histórico
// que chega ao reconectar (messages.set) e em alguns recibos o @lid vem cru, e o CRM chegou a criar contatos
// duplicados com telefone "74032368082963@lid" (131 em 23/11/2026, 120 deles cópia de um contato que já existia).
// Este módulo:
//   1. guarda o mapa lid → telefone (tabela lid_mapa), aprendido das próprias mensagens (remoteJidAlt) e do arquivo
//      lid-mapa.json, que o script lid-mapa.sh gera na VPS a cada 10 min a partir da base da Evolution;
//   2. resolve a chave de uma mensagem para o telefone;
//   3. funde cada contato "@lid" no contato do telefone (mensagens, etiquetas, abas, histórico, follow-up, sombra,
//      anexos, agenda), somando as marcas de "última" e de lido, e apaga a cópia. Roda ao subir e a cada 10 min.
// Nada aqui apaga mensagem: só muda de dono. Não envia nada a ninguém.
const fs = require('fs');
const path = require('path');

const ARQUIVO = path.join(__dirname, 'lid-mapa.json');
const TABELAS = ['mensagens', 'contato_etiquetas', 'contato_abas', 'etapa_historico', 'followup_eventos', 'sombra_turnos', 'anexos_lidos', 'agenda_cfaz'];

let db = null;
let arquivo = { mtime: 0, mapa: {} };

function iniciar(opcoes) {
  db = opcoes.db;
  db.exec(`CREATE TABLE IF NOT EXISTS lid_mapa (
    lid TEXT PRIMARY KEY, telefone TEXT NOT NULL, origem TEXT DEFAULT '',
    visto_em TEXT DEFAULT (datetime('now','localtime')))`);
}

function soLid(jid) {
  const s = String(jid || '');
  return s.endsWith('@lid') ? s.split('@')[0].split(':')[0] : '';
}
function soTelefone(jid) {
  const s = String(jid || '');
  if (!/@s\.whatsapp\.net$|@c\.us$/.test(s)) return '';
  const n = s.split('@')[0].split(':')[0];
  return /^\d{8,15}$/.test(n) ? n : '';
}

function carregarArquivo() {
  try {
    const st = fs.statSync(ARQUIVO);
    if (st.mtimeMs !== arquivo.mtime) {
      const j = JSON.parse(fs.readFileSync(ARQUIVO, 'utf8'));
      arquivo = { mtime: st.mtimeMs, mapa: (j && typeof j === 'object') ? j : {} };
    }
  } catch { arquivo = { mtime: 0, mapa: {} }; }
  return arquivo.mapa;
}

// Guarda (ou corrige) lid → telefone. Devolve true se gravou.
function aprender(lid, telefone, origem) {
  lid = String(lid || '').replace(/@.*$/, '').split(':')[0];
  telefone = String(telefone || '').replace(/\D/g, '');
  if (!/^\d{8,20}$/.test(lid) || !/^\d{8,15}$/.test(telefone)) return false;
  db.prepare(`INSERT INTO lid_mapa (lid, telefone, origem) VALUES (?, ?, ?)
    ON CONFLICT(lid) DO UPDATE SET telefone = excluded.telefone, origem = excluded.origem, visto_em = datetime('now','localtime')
    WHERE lid_mapa.telefone != excluded.telefone`).run(lid, telefone, origem || '');
  return true;
}

// Telefone de uma chave de mensagem: direto, ou traduzindo o @lid (chave alternativa, tabela, arquivo). '' se não souber.
function resolver(key) {
  key = key || {};
  const jid = key.remoteJid || '';
  const tel = soTelefone(jid);
  if (tel) return tel;
  const lid = soLid(jid);
  if (!lid) return '';
  const alt = soTelefone(key.remoteJidAlt || key.senderPn || '');
  if (alt) { aprender(lid, alt, 'mensagem'); return alt; }
  const r = db.prepare('SELECT telefone FROM lid_mapa WHERE lid = ?').get(lid);
  if (r) return r.telefone;
  const a = carregarArquivo()[lid];
  if (a && aprender(lid, a, 'arquivo')) return String(a).replace(/\D/g, '');
  return '';
}

function pendentes() {
  return db.prepare("SELECT id, nome, telefone FROM contatos WHERE telefone LIKE '%@lid'").all();
}

// Funde os contatos "@lid" cujo telefone já se conhece. Devolve { pendentes, fundidos, renomeados, detalhes }.
function fundir() {
  const res = { pendentes: 0, fundidos: 0, renomeados: 0, detalhes: [] };
  const lista = pendentes();
  if (!lista.length) return res;
  carregarArquivo();
  const tx = db.transaction((c, tel) => {
    const lidNum = c.telefone.split('@')[0];
    const alvo = db.prepare('SELECT * FROM contatos WHERE telefone = ?').get(tel);
    const cheio = db.prepare('SELECT * FROM contatos WHERE id = ?').get(c.id);
    if (!alvo) {
      db.prepare("UPDATE contatos SET telefone = ?, nome = CASE WHEN nome = ? THEN '' ELSE nome END, updated_at = datetime('now','localtime') WHERE id = ?").run(tel, lidNum, c.id);
      res.renomeados++;
      res.detalhes.push({ lid: lidNum, telefone: tel, acao: 'renomeado' });
      return;
    }
    let movidas = 0;
    for (const t of TABELAS) {
      movidas += db.prepare(`UPDATE OR IGNORE ${t} SET contato_id = ? WHERE contato_id = ?`).run(alvo.id, c.id).changes;
      db.prepare(`DELETE FROM ${t} WHERE contato_id = ?`).run(c.id); // sobras de chave repetida (mesma etiqueta/aba)
    }
    const nomeCopia = (cheio.nome && cheio.nome !== lidNum) ? cheio.nome : '';
    db.prepare(`UPDATE contatos SET
        nome = CASE WHEN COALESCE(nome,'') = '' THEN ? ELSE nome END,
        pet_nome = CASE WHEN COALESCE(pet_nome,'') = '' THEN COALESCE(?, '') ELSE pet_nome END,
        pet_especie = CASE WHEN COALESCE(pet_especie,'') = '' THEN COALESCE(?, '') ELSE pet_especie END,
        veterinario = CASE WHEN COALESCE(veterinario,'') = '' THEN COALESCE(?, '') ELSE veterinario END,
        etapa = CASE WHEN etapa = 'novo' AND ? != 'novo' THEN ? ELSE etapa END,
        notas = CASE WHEN COALESCE(?, '') = '' THEN notas WHEN COALESCE(notas,'') = '' THEN ? ELSE notas || char(10) || ? END,
        last_message_at = MAX(COALESCE(last_message_at,''), COALESCE(?, '')),
        last_read_at = MAX(COALESCE(last_read_at,''), COALESCE(?, '')),
        ultima_resposta_cliente_at = MAX(COALESCE(ultima_resposta_cliente_at,''), COALESCE(?, '')),
        orcamento_enviado_at = MAX(COALESCE(orcamento_enviado_at,''), COALESCE(?, '')),
        manual_unread = MAX(COALESCE(manual_unread,0), COALESCE(?, 0)),
        pinado = MAX(COALESCE(pinado,0), COALESCE(?, 0)),
        favorito = MAX(COALESCE(favorito,0), COALESCE(?, 0)),
        foto_url = CASE WHEN COALESCE(foto_url,'') = '' THEN COALESCE(?, '') ELSE foto_url END,
        updated_at = datetime('now','localtime')
      WHERE id = ?`).run(
      nomeCopia, cheio.pet_nome, cheio.pet_especie, cheio.veterinario, cheio.etapa || 'novo', cheio.etapa || 'novo',
      cheio.notas, cheio.notas, cheio.notas, cheio.last_message_at, cheio.last_read_at, cheio.ultima_resposta_cliente_at,
      cheio.orcamento_enviado_at, cheio.manual_unread, cheio.pinado, cheio.favorito, cheio.foto_url, alvo.id);
    db.prepare('DELETE FROM contatos WHERE id = ?').run(c.id);
    res.fundidos++;
    res.detalhes.push({ lid: lidNum, telefone: tel, acao: 'fundido', em: alvo.id, movidas });
  });
  for (const c of lista) {
    const tel = resolver({ remoteJid: c.telefone });
    if (!tel) { res.pendentes++; continue; }
    try { tx(c, tel); }
    catch (e) { res.pendentes++; console.error(`[LID] erro ao fundir ${c.telefone} em ${tel}:`, e.message); }
  }
  if (res.fundidos || res.renomeados) console.log(`[LID] contatos @lid: ${res.fundidos} fundidos no contato do telefone, ${res.renomeados} renomeados, ${res.pendentes} ainda sem tradução`);
  return res;
}

module.exports = { iniciar, resolver, aprender, fundir, pendentes, soLid, soTelefone, ARQUIVO };
