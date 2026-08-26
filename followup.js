'use strict';

const fs = require('fs');
const path = require('path');
const MEDIA_DIR = path.join(__dirname, 'media');

// ─── Motor de follow-up do funil de orçamentos ─────────────────────────────────
// Vigia quem recebeu orçamento (PDF) e não fechou. Marca nível no banco.
// FASE 1: só marca. Não envia mensagem para ninguém. O envio mora em alerta.js.

const ABERTURA = 8;      // expediente começa 8h
const FECHAMENTO = 18;   // expediente termina 18h
// dias úteis: seg(1) a sáb(6). domingo(0) não conta.

const REGRAS = {
  minutosClienteEsperando: 30,   // 🔴 cliente falou por último e ninguém respondeu
  horasSemResposta: 12,          // 🟡 mandamos e o cliente sumiu
  horasSegundaTentativa: 48,     // ⚪ segunda tentativa depois do 🟡
  diasArquivarSegundaTentativa: 5, // 5 dias em ⚪ sem resposta → arquiva (Finalizado "sumiu"). Decisão do Thiago 23/08.

  // Nada anterior a esta data é cobrado. O histórico velho fica visível no CRM,
  // mas em silêncio: orçamento de meses atrás pode ser paciente que já morreu.
  corte: '2026-08-17 00:00:00',

  // Mensagem curta do cliente ("ok", "obrigado", emoji) encerra conversa e não
  // é alguém esperando resposta. Não vira 🔴 — cai na régua normal de 12h.
  tamanhoCortesia: 10,
};

// Se qualquer uma destas palavras aparecer na conversa, o contato nunca é
// cobrado — em nenhum nível, para sempre. Vale mais deixar passar uma venda
// do que perguntar "fechou o orçamento?" para quem perdeu o animal.
const PALAVRAS_LUTO = [
  'obito', 'óbito', 'faleceu', 'falecido', 'falecida', 'morreu', 'morreram',
  'eutanasia', 'eutanásia', 'sacrificado', 'sacrificada', 'nao resistiu',
  'não resistiu', 'veio a obito', 'veio a óbito', 'descansou', 'partiu ontem',
  'perdemos ele', 'perdemos ela', 'perdi ele', 'perdi ela', 'meus pesames',
  'meus pêsames', 'sentimos muito pela perda',
];

// Cliente disse que não vai fazer. Sai da fila de vez: o contato vai para
// "Finalizado" e o follow-up zera. Só vale para mensagem DO CLIENTE chegada
// depois do orçamento — nunca para texto nosso.
const PALAVRAS_DESISTENCIA = [
  'nao quero mais', 'não quero mais', 'nao vou fazer', 'não vou fazer',
  'nao vamos fazer', 'não vamos fazer', 'desisti', 'desistimos',
  'ja fiz em outro', 'já fiz em outro', 'fiz em outro lugar',
  'ja marquei em outro', 'já marquei em outro', 'marquei em outro lugar',
  'vou deixar pra depois', 'vou deixar para depois', 'deixa pra outra hora',
  'nao tenho condicoes', 'não tenho condições', 'muito caro pra mim',
  'nao vai ser possivel', 'não vai ser possível',
  'no momento nao', 'no momento não', 'agora nao', 'agora não',
  'por enquanto nao', 'por enquanto não', 'nao por enquanto', 'não por enquanto',
];

// Cliente ENCERRANDO a conversa (não está esperando nada): agradecimento,
// "ok", "vou pensar", "te retorno". Não vira 🔴 — cai na régua de 12h.
// Regex por radical (sem \b no fim), sobre texto sem acento e minúsculo.
const RE_FECHAMENTO = /(^|\W)(obrigad|obg|agrade|valeu|blz|beleza|combinado|perfeito|certo|entendi|ok|okay|ta bom|tabom|tudo bem|show|otimo|joia|top|vou pensar|vou conversar|vou ver|vou verificar|vou confirmar|vou so |vou ja |verificando|confirmando|conversando|analisando|conferindo|te retorno|retorno depois|entro em contato|volto a falar|volto falar|qualquer coisa|bom dia|boa tarde|boa noite|ate mais|abraco|bjs|beijo|grata|grato|finalizamos o seu atendimento|finalizamos seu atendimento|atendimento encerrado|atendimento finalizado|pesquisa de satisfacao|protocolo de atendimento)/;
const RE_PERGUNTA = /\?|quanto|qual |quando|como |pode |consegue|teria|gostaria|preciso|queria|valor|preco|horario|agend|marcar/;
const normalizar = s => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();

// É mensagem de encerramento? Curta, ou bate no radical, e NÃO tem cara de pergunta.
function ehFechamento(texto, tamanho) {
  const t = normalizar(texto);
  if (!t) return false;
  // Robô de OUTRA empresa encerrando ("Finalizamos o seu atendimento...") — qualquer tamanho.
  if (/finalizamos (o )?seu atendimento|atendimento (encerrado|finalizado)|pesquisa de satisfacao|protocolo de atendimento/.test(t)) return true;
  if ((tamanho || t.length) <= REGRAS.tamanhoCortesia) return true;
  if (RE_PERGUNTA.test(t)) return false;
  if (t.length > 120) return false;
  return RE_FECHAMENTO.test(t);
}

// A Athena (atendente virtual fora do horário) responde pelo MESMO número da
// recepção, então as mensagens dela entram como 'out'. Para a régua, Athena
// não conta como resposta humana: cliente que escreveu de noite e só falou
// com ela continua "esperando resposta" e vira 🔴 na primeira meia hora útil.
// Toda mensagem dela começa com este marcador (athena-bot/server.js → enviar()).
const ATHENA_MARCA = 'Assistente virtual Athena';
// Saudação automática do WhatsApp Business da recepção ("XR Diagnóstico agradece seu
// contato. Como podemos ajudar?") — também sai como 'out' e também NÃO é resposta humana.
const SAUDACAO_MARCA = 'agradece seu contato';

// Confirmação curta do time = fechou. "marcado", "agendado!", "ok, reservado",
// "confirmado", "anotado", "combinado" sozinhos na mensagem (≤ 40 chars).
// Caso real: vet pede "reservar consultório dia 26/8 9h", time responde "marcado",
// cliente "mt obg" — estava virando 🟡 "orçamento sem resposta".
// Só palavras que significam AGENDAMENTO feito. "Combinado"/"anotado"/"fechado" ficaram
// de fora: o time usa "Combinado" como "ok" depois de "vou pensar e te retorno" (auditado: 10 falsos).
const RE_CONFIRMACAO_CURTA = /^\W*(ok[,.!\s]*|certo[,.!\s]*|pronto[,.!\s]*)?(marcad[oa]|agendad[oa]|reservad[oa]|confirmad[oa])[\s!.👍✅]*$/i;
const ehConfirmacaoCurta = t => { const s = String(t || '').trim(); return s.length <= 40 && RE_CONFIRMACAO_CURTA.test(s); };

// ─── Horário útil ──────────────────────────────────────────────────────────────
const ehDiaUtil = d => d.getDay() >= 1 && d.getDay() <= 6;
const emHorarioUtil = d => ehDiaUtil(d) && d.getHours() >= ABERTURA && d.getHours() < FECHAMENTO;

// Próximo instante em que o expediente está aberto (hoje mais tarde, ou o próximo dia útil).
function proximaAbertura(d) {
  const x = new Date(d.getTime());
  if (ehDiaUtil(x) && x.getHours() < ABERTURA) { x.setHours(ABERTURA, 0, 0, 0); return x; }
  do { x.setDate(x.getDate() + 1); } while (!ehDiaUtil(x));
  x.setHours(ABERTURA, 0, 0, 0);
  return x;
}

// Se o prazo cair fora do expediente, empurra para a primeira hora útil seguinte.
// É a regra "12h ou na primeira hora útil do dia seguinte".
const quandoAlertar = d => emHorarioUtil(d) ? new Date(d.getTime()) : proximaAbertura(d);

// Minutos de expediente decorridos entre dois instantes (ignora noite e domingo).
function minutosUteisEntre(ini, fim) {
  let total = 0;
  let cur = new Date(ini.getTime());
  while (cur < fim) {
    if (emHorarioUtil(cur)) {
      const fimExpediente = new Date(cur.getTime());
      fimExpediente.setHours(FECHAMENTO, 0, 0, 0);
      const ate = fim < fimExpediente ? fim : fimExpediente;
      total += (ate - cur) / 60000;
      cur = ate;
      if (cur >= fim) break;
    } else {
      cur = proximaAbertura(cur);
    }
  }
  return Math.floor(total);
}

// ─── Datas no formato do banco ('YYYY-MM-DD HH:MM:SS', horário local) ──────────
function dt(s) {
  if (!s) return null;
  const d = new Date(String(s).replace(' ', 'T'));
  return isNaN(d.getTime()) ? null : d;
}
function sql(d) {
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
function humano(min) {
  if (min < 60) return `${min}min`;
  const h = Math.floor(min / 60);
  return h < 24 ? `${h}h` : `${Math.floor(h / 24)}d`;
}

// ─── Migrations ────────────────────────────────────────────────────────────────
function initFollowup(db) {
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_nivel INTEGER DEFAULT 0"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_desde TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_motivo TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_alertado_em TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN orcamento_modalidade TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_snooze_ate TEXT DEFAULT ''"); } catch {}
  try { db.exec("ALTER TABLE contatos ADD COLUMN followup_ignorar INTEGER DEFAULT 0"); } catch {} // labs, fornecedores, parceiros: nunca entram no funil
  try { db.exec("CREATE INDEX IF NOT EXISTS idx_contatos_followup ON contatos(followup_nivel)"); } catch {}
  // Histórico do follow-up (pra métricas: quem foi cobrado, quem recuperou, por que perdeu)
  try { db.exec(`CREATE TABLE IF NOT EXISTS followup_eventos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contato_id INTEGER NOT NULL,
    tipo TEXT NOT NULL,          -- 'nivel' | 'encerrado' | 'comando'
    valor TEXT DEFAULT '',       -- nivel: '1','2','3' · encerrado: motivo · comando: marcou/desistiu/ok
    created_at TEXT DEFAULT (datetime('now','localtime'))
  )`); } catch {}
  try { db.exec("CREATE INDEX IF NOT EXISTS idx_fu_eventos_contato ON followup_eventos(contato_id)"); } catch {}
  try { db.exec("CREATE INDEX IF NOT EXISTS idx_fu_eventos_tipo ON followup_eventos(tipo, created_at)"); } catch {}
}

function registrarEvento(db, contatoId, tipo, valor) {
  try { db.prepare("INSERT INTO followup_eventos (contato_id, tipo, valor) VALUES (?, ?, ?)").run(contatoId, tipo, String(valor == null ? '' : valor)); } catch (e) { console.error('[CRM] evento:', e.message); }
}

// ─── Correção do bug do laudo ──────────────────────────────────────────────────
// O laudo também sai em PDF, depois do exame. O auto-move tratava esse PDF como
// orçamento novo e jogava o contato de "Agendado" de volta para "Orçamento".
// Devolve para "Agendado" quem entrou em Orçamento vindo de Agendado.
function corrigirLaudos(db) {
  const ultimaTransicao = db.prepare(`
    SELECT etapa_de FROM etapa_historico
    WHERE contato_id = ? ORDER BY id DESC LIMIT 1
  `);
  const alvos = db.prepare("SELECT id, nome, telefone FROM contatos WHERE etapa = 'orcamento'").all();

  let corrigidos = 0;
  const voltar = db.prepare("UPDATE contatos SET etapa = 'agendado', followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE id = ?");
  const logar = db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'orcamento', 'agendado')");

  db.transaction(() => {
    for (const c of alvos) {
      const t = ultimaTransicao.get(c.id);
      if (t && t.etapa_de === 'agendado') {
        voltar.run(c.id);
        logar.run(c.id);
        corrigidos++;
      }
    }
  })();

  if (corrigidos) console.log(`[CRM] Follow-up: ${corrigidos} contatos devolvidos para Agendado (PDF era laudo, não orçamento)`);
  return corrigidos;
}

// ─── Encerramento automático ───────────────────────────────────────────────────
// Varre quem está em Orçamento: se o CLIENTE, depois do orçamento, disse que
// desistiu ou que o animal morreu, o contato sai da fila (vai para Finalizado)
// e nunca mais é cobrado. É o que fecha o ciclo do follow-up: o time cobra,
// o cliente responde "não quero mais" / "ele faleceu", e o card some sozinho.
function encerrarPerdidos(db) {
  const mk = lista => ({
    cond: lista.map(() => 'LOWER(texto) LIKE ?').join(' OR '),
    args: lista.map(p => `%${p}%`),
  });
  const luto = mk(PALAVRAS_LUTO);
  const desist = mk(PALAVRAS_DESISTENCIA);

  const acha = (c, m) => db.prepare(
    `SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'in'
     AND created_at >= ? AND (${m.cond}) LIMIT 1`
  ).get(c.id, c.orcamento_enviado_at || c.created_at || '', ...m.args);

  const alvos = db.prepare("SELECT id, nome, telefone, etapa, orcamento_enviado_at, created_at FROM contatos WHERE etapa = 'orcamento'").all();
  const finalizar = db.prepare("UPDATE contatos SET etapa = 'finalizado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?");
  const logar = db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'orcamento', 'finalizado')");

  // Laudo legado: PDF de antes do filtro de nome (texto '[document]'). Se o time
  // tinha mandado "Exame agendado" antes do PDF, ou fala em laudo depois dele,
  // o PDF era laudo — exame feito, não orçamento. Só vale pra PDF sem nome;
  // PDF novo já passa pelo filtro do webhook.
  const ultimoPdf = db.prepare("SELECT texto, media_path FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND tipo = 'document' ORDER BY id DESC LIMIT 1");
  const agendadoAntes = db.prepare("SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at < ? AND created_at >= datetime(?, '-45 days') AND LOWER(texto) LIKE '%exame agendado%' LIMIT 1");
  const falaLaudoDepois = db.prepare("SELECT texto FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at >= ? AND LOWER(texto) LIKE '%laudo%' LIMIT 5");
  // Time falando como quem acabou de fazer o exame, até 10 dias antes do PDF.
  const posExameAntes = db.prepare("SELECT texto FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at < ? AND created_at >= datetime(?, '-10 days') ORDER BY id DESC LIMIT 25");
  const RE_LAUDO = /segue.{0,30}laudo|laudo.{0,20}(anexo|pronto|liberad|dispon|finaliz)|envi\w+.{0,15}laudo|laudo (do|da) /i;
  const RE_POS_EXAME = /(ja|já) (esta|está) liberad|liberad[ao] (pra|para) (ir|buscar|casa)|recupera[cç][aã]o anest|acordou|acordando|pode (vir )?buscar|correu (tudo )?bem|terminou o exame|saiu do exame|exame (foi|ja foi|já foi) (feito|realizad)|(ja|já) (esta|está) pront[ao]/i;
  const PDF_GRANDE = 2.5 * 1024 * 1024; // orçamento do gerador tem ~680KB; laudo com imagens passa fácil de 2,5MB
  const tamanhoPdf = p => { try { return p && p.media_path ? fs.statSync(path.join(MEDIA_DIR, p.media_path)).size : 0; } catch { return 0; } };
  const ehLaudoLegado = c => {
    const p = ultimoPdf.get(c.id);
    if (!p || p.texto !== '[document]') return false;   // PDF novo já tem nome e passou pelo filtro
    const ref = c.orcamento_enviado_at || c.created_at || '';
    if (agendadoAntes.get(c.id, ref, ref)) return 'exame agendado antes do PDF';
    if (falaLaudoDepois.all(c.id, ref).some(r => RE_LAUDO.test(String(r.texto || '')))) return 'fala em laudo depois do PDF';
    if (posExameAntes.all(c.id, ref, ref).some(r => RE_POS_EXAME.test(String(r.texto || '')))) return 'pós-exame antes do PDF';
    if (tamanhoPdf(p) > PDF_GRANDE) return 'PDF grande (laudo com imagens)';
    return false;
  };

  const encerrados = [];
  db.transaction(() => {
    for (const c of alvos) {
      const porLuto = acha(c, luto);
      const porDesist = !porLuto && acha(c, desist);
      const porLaudo = !porLuto && !porDesist && ehLaudoLegado(c);
      if (porLuto || porDesist || porLaudo) {
        finalizar.run(c.id);
        logar.run(c.id);
        const motivo = porLuto ? 'luto' : porDesist ? 'desistiu' : 'exame já feito: ' + porLaudo;
        encerrados.push({ id: c.id, nome: c.nome, telefone: c.telefone, motivo });
        registrarEvento(db, c.id, 'encerrado', motivo);
      }
    }
  })();

  for (const e of encerrados) console.log(`[CRM] Follow-up: ${e.nome || e.telefone} encerrado (${e.motivo})`);
  return encerrados;
}

// ─── Fechamento por confirmação curta do time ──────────────────────────────────
// Quem está em Orçamento e recebeu do time, depois do orçamento, um "marcado"/
// "agendado"/"reservado" seco → vai pra Agendado (saiu da fila). Roda no vigia.
function fecharConfirmados(db) {
  const alvos = db.prepare("SELECT id, nome, telefone, etapa, orcamento_enviado_at, created_at FROM contatos WHERE etapa = 'orcamento'").all();
  const outsDepois = db.prepare("SELECT texto, created_at FROM mensagens WHERE contato_id = ? AND direcao = 'out' AND created_at >= ? AND LENGTH(texto) <= 40 ORDER BY id");
  const mover = db.prepare("UPDATE contatos SET etapa = 'agendado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?");
  const logar = db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'orcamento', 'agendado')");
  const fechados = [];
  db.transaction(() => {
    for (const c of alvos) {
      const ref = c.orcamento_enviado_at || c.created_at || '';
      const hit = outsDepois.all(c.id, ref).find(m => ehConfirmacaoCurta(m.texto));
      if (!hit) continue;
      mover.run(c.id); logar.run(c.id);
      registrarEvento(db, c.id, 'encerrado', 'agendado (confirmação curta: "' + String(hit.texto).trim().slice(0, 20) + '")');
      fechados.push({ id: c.id, nome: c.nome, telefone: c.telefone, texto: hit.texto });
    }
  })();
  for (const f of fechados) console.log(`[CRM] Follow-up: ${f.nome || f.telefone} → Agendado (time confirmou: "${String(f.texto).trim().slice(0, 20)}")`);
  return fechados;
}

// ─── Decisão ───────────────────────────────────────────────────────────────────
// Função pura: dado o contato e a última mensagem da conversa, qual é o nível.
// Separada do banco de propósito — é isso que permite simular em cima do
// histórico de produção sem escrever nada.
function decidir(contato, ultimaMsg, agora = new Date()) {
  const nada = (por) => ({ nivel: 0, motivo: '', desde: null, ignoradoPor: por || '' });
  if (!ultimaMsg) return nada('sem mensagem');

  // Luto: nunca cobra, em nenhuma hipótese.
  if (contato.luto) return nada('luto');

  // Corte: conversa parada desde antes do corte fica em silêncio.
  const quandoUltima = dt(ultimaMsg.created_at);
  const corte = dt(REGRAS.corte);
  if (corte && quandoUltima && quandoUltima < corte) return nada('anterior ao corte');

  if (ultimaMsg.direcao === 'in') {
    // Cliente falou por último. A bola é nossa — a menos que tenha sido só cortesia.
    if (!quandoUltima) return nada('data inválida');
    const cortesia = ultimaMsg.tipo === 'text' && ehFechamento(ultimaMsg.texto, ultimaMsg.tamanho);
    if (!cortesia) {
      const min = minutosUteisEntre(quandoUltima, agora);
      if (min < REGRAS.minutosClienteEsperando) return nada('dentro do prazo');
      // O prazo é em minutos úteis, mas o texto mostra tempo real ("há 2d" = 2 dias de relógio).
      const realMin = Math.floor((agora - quandoUltima) / 60000);
      return { nivel: 1, motivo: `cliente esperando resposta há ${humano(realMin)}`, desde: quandoUltima, ignoradoPor: '' };
    }
    // Cortesia curta: segue para a régua de 12h abaixo, sem urgência.
  }

  // Falamos por último. A bola é do cliente — mas o toque é nosso.
  const base = dt(ultimaMsg.created_at);
  const orc = dt(contato.orcamento_enviado_at);
  const ref = (orc && base && orc > base) ? orc : base;
  if (!ref) return nada('data inválida');

  const alvo = quandoAlertar(new Date(ref.getTime() + REGRAS.horasSemResposta * 3600000));
  if (agora < alvo) return nada('dentro do prazo');

  const alvo2 = new Date(alvo.getTime() + REGRAS.horasSegundaTentativa * 3600000);
  const nivel = agora >= alvo2 ? 3 : 2;
  const realMin = Math.floor((agora - ref) / 60000); // tempo real, pra leitura
  // (Auditado 23/08: o questionário pré-anestésico vai junto do PDF em 97% dos casos —
  // é template, não sinal de aceite. O rótulo "aceitou" foi removido por isso.)
  const rotulo = 'orçamento sem resposta';
  return {
    nivel,
    motivo: nivel === 3
      ? `${rotulo} há ${humano(realMin)} — segunda tentativa`
      : `${rotulo} há ${humano(realMin)}`,
    desde: alvo,
    ignoradoPor: '',
  };
}

// ─── Avaliação ─────────────────────────────────────────────────────────────────
// Recalcula o nível de todo mundo do zero a cada rodada. Idempotente de propósito:
// sem estado acumulado, sem risco de o contador desandar com o tempo.
function avaliar(db, agora = new Date()) {
  // Números internos da XR nunca entram na fila (recepção falando com ela mesma etc.).
  const internos = ['553182043117', '553183283626', '553183746380', '553184505939' /* Xrfinanceiro */];
  const alvos = db.prepare(`
    SELECT id, nome, telefone, orcamento_enviado_at, followup_nivel, followup_snooze_ate
    FROM contatos
    WHERE etapa = 'orcamento' AND orcamento_enviado_at != '' AND COALESCE(followup_ignorar, 0) = 0
  `).all().filter(c => !internos.includes(String(c.telefone).replace(/\D/g, '')));

  // Última mensagem HUMANA da conversa: ignora o que a Athena mandou.
  const ultimaMsg = db.prepare(`
    SELECT direcao, created_at, tipo, LENGTH(texto) AS tamanho, SUBSTR(texto, 1, 200) AS texto FROM mensagens
    WHERE contato_id = ? AND NOT (direcao = 'out' AND (texto LIKE ? OR texto LIKE ?))
    ORDER BY id DESC LIMIT 1`);
  const athenaLike = '%' + ATHENA_MARCA + '%';
  const saudacaoLike = '%' + SAUDACAO_MARCA + '%';
  const condLuto = PALAVRAS_LUTO.map(() => "LOWER(texto) LIKE ?").join(" OR ");
  const temLuto = db.prepare(`SELECT 1 FROM mensagens WHERE contato_id = ? AND (${condLuto}) LIMIT 1`);
  const argsLuto = PALAVRAS_LUTO.map(p => `%${p}%`);
  const gravar = db.prepare("UPDATE contatos SET followup_nivel = ?, followup_desde = ?, followup_motivo = ? WHERE id = ?");
  // Arquivamento: ⚪ há N dias (duas cobranças feitas, cliente mudo) → Finalizado. Se ele voltar a falar, reabre sozinho.
  const arquivar = db.prepare("UPDATE contatos SET etapa = 'finalizado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?");
  const logArquivo = db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, 'orcamento', 'finalizado')");
  const msArquivar = (REGRAS.horasSegundaTentativa * 3600 + REGRAS.diasArquivarSegundaTentativa * 86400) * 1000; // desde o início do 🟡

  const mudancas = [];

  for (const c of alvos) {
    const m = ultimaMsg.get(c.id, athenaLike, saudacaoLike);
    c.luto = !!temLuto.get(c.id, ...argsLuto);
    let { nivel, motivo, desde } = decidir(c, m, agora);
    if (nivel === 3 && desde && (agora - desde) >= msArquivar) {
      arquivar.run(c.id); logArquivo.run(c.id);
      registrarEvento(db, c.id, 'encerrado', `arquivado: sem resposta após 2 cobranças (${REGRAS.diasArquivarSegundaTentativa}d em ⚪)`);
      console.log(`[CRM] Follow-up: ${c.nome || c.telefone} arquivado (sem resposta após 2 cobranças)`);
      continue;
    }
    // "ok 123" pela recepção: silencia por um tempo (já ligaram, já resolveram por fora).
    if (nivel > 0 && c.followup_snooze_ate && dt(c.followup_snooze_ate) && agora < dt(c.followup_snooze_ate)) { nivel = 0; motivo = ''; desde = null; }

    if (nivel !== c.followup_nivel) {
      gravar.run(nivel, desde ? sql(desde) : '', motivo, c.id);
      mudancas.push({ id: c.id, nome: c.nome, telefone: c.telefone, de: c.followup_nivel, para: nivel, motivo });
    } else if (nivel > 0) {
      gravar.run(nivel, desde ? sql(desde) : '', motivo, c.id); // atualiza o texto do "há Xh"
    }
  }

  // Internos nunca ficam marcados.
  for (const n of internos) db.prepare("UPDATE contatos SET followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE followup_nivel > 0 AND REPLACE(REPLACE(REPLACE(telefone,'+',''),'-',''),' ','') LIKE ?").run('%' + n.slice(-11) + '%');

  // Quem saiu de Orçamento (fechou, arquivou) para de ser cobrado.
  db.prepare("UPDATE contatos SET followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE followup_nivel > 0 AND etapa != 'orcamento'").run();

  return mudancas;
}

// ─── Vigia ─────────────────────────────────────────────────────────────────────
function iniciarVigia(db, opts = {}) {
  const intervaloMs = opts.intervaloMs || 5 * 60 * 1000;
  const aoMudar = opts.aoMudar || null;

  const rodada = () => {
    try {
      encerrarPerdidos(db);
      fecharConfirmados(db);
      const mudancas = avaliar(db);
      const subiram = mudancas.filter(m => m.para > m.de);
      const icone = n => n === 1 ? '🔴' : n === 2 ? '🟡' : '⚪';
      for (const m of subiram) { console.log(`[CRM] Follow-up ${icone(m.para)} ${m.nome || m.telefone}: ${m.motivo}`); registrarEvento(db, m.id, 'nivel', m.para); }
      if (aoMudar && subiram.length) aoMudar(subiram);
    } catch (e) {
      console.error('[CRM] Follow-up erro na rodada:', e.message);
    }
  };

  rodada();
  return setInterval(rodada, intervaloMs);
}

// ─── Comandos da recepção (respondidos na conversa do robô) ────────────────────
function executarComando(db, cmd, id) {
  if (cmd === 'ajuda' || cmd === '?') {
    return '🤖 *Comandos do follow-up*\n\n' +
      '• *marcou 123* — marcou o exame → vai pra Agendado\n' +
      '• *desistiu 123* — não vai fazer → sai da fila\n' +
      '• *ok 123* — já resolvi por fora (liguei etc.) → silencia por 24h\n' +
      '• *ignorar 123* — não é cliente (lab, fornecedor) → sai pra sempre\n\n' +
      'O número é o *#id* que aparece no alerta.';
  }
  // "ok" / "feito" soltos são conversa normal — ignora em silêncio. "perdeu" solto, avisa.
  if (!id && (cmd === 'ok' || cmd === 'feito' || cmd === 'resolvido')) return null;
  if (!id) return `Faltou o número. Ex.: *${cmd} 123* (o #id que vem no alerta). Mande *ajuda* pra ver os comandos.`;

  const c = db.prepare('SELECT id, nome, telefone, pet_nome, etapa FROM contatos WHERE id = ?').get(id);
  if (!c) return `Não achei o contato *#${id}*. Confere o número no alerta.`;
  const quem = (c.nome || c.telefone) + (c.pet_nome ? ` · ${c.pet_nome}` : '');
  registrarEvento(db, id, 'comando', cmd);

  if (cmd === 'ignorar') {
    db.prepare("UPDATE contatos SET followup_ignorar = 1, etapa = 'diversos', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
    db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'diversos')").run(id, c.etapa);
    console.log(`[CRM] Comando ignorar #${id} (${quem})`);
    return `🚫 *${quem}* (#${id}) marcado como "não é cliente" (lab/fornecedor). Foi pra Diversos e nunca mais entra no follow-up.`;
  }

  if (cmd === 'desistiu' || cmd === 'desistiram' || cmd === 'perdeu' || cmd === 'perdido') {
    db.prepare("UPDATE contatos SET etapa = 'finalizado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
    db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'finalizado')").run(id, c.etapa);
    console.log(`[CRM] Comando perdeu #${id} (${quem})`);
    return `✔️ *${quem}* (#${id}) desistiu. Saiu da fila.`;
  }
  if (cmd === 'marcou' || cmd === 'fechou' || cmd === 'agendou') {
    db.prepare("UPDATE contatos SET etapa = 'agendado', followup_nivel = 0, followup_desde = '', followup_motivo = '', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
    db.prepare("INSERT INTO etapa_historico (contato_id, etapa_de, etapa_para) VALUES (?, ?, 'agendado')").run(id, c.etapa);
    console.log(`[CRM] Comando fechou #${id} (${quem})`);
    return `🎉 *${quem}* (#${id}) marcou! Foi pra Agendado.`;
  }
  const ate = new Date(Date.now() + 24 * 3600000);
  db.prepare("UPDATE contatos SET followup_snooze_ate = ?, followup_nivel = 0, followup_desde = '', followup_motivo = '' WHERE id = ?").run(sql(ate), id);
  console.log(`[CRM] Comando ok #${id} (${quem}) snooze até ${sql(ate)}`);
  return `👍 *${quem}* (#${id}) silenciado por 24h. Se o cliente responder, volta sozinho.`;
}

module.exports = {
  executarComando,
  REGRAS, ABERTURA, FECHAMENTO,
  emHorarioUtil, proximaAbertura, quandoAlertar, minutosUteisEntre,
  initFollowup, corrigirLaudos, encerrarPerdidos, fecharConfirmados, ehConfirmacaoCurta, decidir, avaliar, iniciarVigia,
  PALAVRAS_LUTO, PALAVRAS_DESISTENCIA, ATHENA_MARCA, SAUDACAO_MARCA, ehFechamento, registrarEvento,
};
