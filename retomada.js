'use strict';

// ─── Retomada de orçamento (a mensagem pronta para quem sumiu) ─────────────────
// O estudo das conversas (11/11/2026) mostrou que ~125 orçamentos de TC/RM por mês
// não viram exame e só 5% recebem uma segunda mensagem. O follow-up já avisava
// QUEM estava parado; faltava O QUE dizer. Este módulo:
//   1. conta quantas retomadas a recepção já fez no ciclo atual do orçamento;
//   2. sugere o texto da próxima (1ª no dia seguinte, 2ª ~2 dias depois, 3ª e última ~4 dias depois);
//   3. registra a retomada sozinho quando a recepção escreve para um contato 🟡/⚪;
//   4. mede o resultado (responderam, agendaram).
// NADA é enviado por aqui. Quem envia é sempre uma pessoa, pelo WhatsApp da recepção.

const REGRAS = {
  maximo: 3,                  // três mensagens e encerra
  diasAteSegunda: 2,          // 2ª mensagem: N dias depois da 1ª  (≈ 3º dia do orçamento)
  diasAteTerceira: 4,         // 3ª mensagem: N dias depois da 2ª  (≈ 7º dia)
  diasArquivarAposUltima: 2,  // sem resposta à última → Finalizado
  horasMesmaRetomada: 12,     // várias linhas seguidas contam como UMA retomada
  // ↓ Ajustar quando a condição do mês mudar. Aparece nos textos sugeridos.
  condicaoPagamento: 'parcelar em até 10x sem juros no cartão ou dar 5% de desconto à vista (pix ou dinheiro)',
  validadeDias: 15,           // por quanto tempo as condições ficam seguras depois do encerramento
};

const ATHENA_MARCA = 'Assistente virtual Athena';
const SAUDACAO_MARCA = 'agradece seu contato';
const normalizar = s => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();
const dt = s => { if (!s) return null; const d = new Date(String(s).replace(' ', 'T')); return isNaN(d.getTime()) ? null : d; };
const DIA = 86400000;

// Mesmas palavras que movem para "Agendado" (inclui o plural "Exames agendados").
const RE_AGENDOU = /exames? agendados?|exames? marcados?|agendamos|agendad[oa]s? para|marcad[oa]s? para|confirmad[oa]s? para/;

// ─── Ciclo e contagem ──────────────────────────────────────────────────────────
// Ciclo = desde o orçamento OU desde a última mensagem "de verdade" do cliente
// (não vale "ok", "obrigada", "vou ver"). Se o cliente volta a conversar e some
// de novo, a contagem recomeça do 1.
function inicioDoCiclo(db, c) {
  const followup = require('./followup');
  const orc = dt(c.orcamento_enviado_at);
  let inicio = orc;
  const ins = db.prepare("SELECT texto, tipo, created_at, LENGTH(texto) AS tam FROM mensagens WHERE contato_id = ? AND direcao = 'in' AND created_at > ? ORDER BY id DESC LIMIT 30")
    .all(c.id, c.orcamento_enviado_at || '');
  for (const m of ins) {
    const cortesia = m.tipo === 'text' && followup.ehFechamento(m.texto, m.tam);
    if (!cortesia) { const t = dt(m.created_at); if (t && (!inicio || t > inicio)) inicio = t; break; }
  }
  return inicio;
}

function estado(db, c, agora = new Date()) {
  const inicio = inicioDoCiclo(db, c);
  const p = n => String(n).padStart(2, '0');
  const sql = d => `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  const evs = db.prepare("SELECT created_at FROM followup_eventos WHERE contato_id = ? AND tipo = 'retomada' AND created_at > ? ORDER BY id")
    .all(c.id, inicio ? sql(inicio) : '');
  const ultima = evs.length ? dt(evs[evs.length - 1].created_at) : null;
  const feitas = evs.length;
  // Quando a próxima mensagem fica devida (null = já pode / não se aplica)
  let proximaEm = null;
  if (feitas === 1 && ultima) proximaEm = new Date(ultima.getTime() + REGRAS.diasAteSegunda * DIA);
  else if (feitas === 2 && ultima) proximaEm = new Date(ultima.getTime() + REGRAS.diasAteTerceira * DIA);
  return { inicio, feitas, ultima, etapa: Math.min(feitas + 1, REGRAS.maximo), proximaEm, esgotado: feitas >= REGRAS.maximo };
}

// Chamado quando sai uma mensagem HUMANA da recepção para um contato.
// Conta como retomada se o contato estava 🟡/⚪ (orçamento parado) e ainda não
// houve retomada nas últimas horas. Mensagem de agendamento não é retomada.
function registrarSeRetomada(db, contatoId, texto, tipo) {
  const t = String(texto || '');
  if (t.includes(ATHENA_MARCA) || t.includes(SAUDACAO_MARCA)) return null;
  if (tipo === 'document' && /or[cç]amento/i.test(t)) return null;      // orçamento novo = ciclo novo, não retomada
  if (RE_AGENDOU.test(normalizar(t))) return null;
  const c = db.prepare("SELECT id, etapa, followup_nivel, orcamento_enviado_at FROM contatos WHERE id = ?").get(contatoId);
  if (!c || c.etapa !== 'orcamento' || !(c.followup_nivel === 2 || c.followup_nivel === 3)) return null;
  const recente = db.prepare("SELECT 1 FROM followup_eventos WHERE contato_id = ? AND tipo = 'retomada' AND created_at > datetime('now','localtime', ?) LIMIT 1")
    .get(contatoId, `-${REGRAS.horasMesmaRetomada} hours`);
  if (recente) return null;
  const e = estado(db, c);
  if (e.esgotado) return null;
  require('./followup').registrarEvento(db, contatoId, 'retomada', String(e.etapa));
  console.log(`[RETOMADA] contato ${contatoId}: ${e.etapa}ª mensagem registrada`);
  return e.etapa;
}

// ─── Sinais da conversa (para escolher o texto) ────────────────────────────────
const RE = {
  pensar: /vou pensar|vou ver|vou conversar|vou verificar|vou analisar|te retorno|retorno depois|dou retorno|te aviso|te falo|entro em contato|vou falar com|mais tarde confirmo/,
  pagamento: /caro|desconto|parcel|dividir|vezes|a vista|forma de pagamento|condic(ao|oes)|orcamento melhor|mais barato|melhorar (esse |o )?valor|financeir/,
  pendenciaCli: /vou fazer os exames|fazer o exame de sangue|aguardando (o )?resultado|esperando (o )?resultado|assim que (sair|ficar pronto|tiver)|vou (buscar|pegar) os exames|ainda nao fez|vai fazer os exames|falta (o |a )?(eletro|eco|sangue|hemograma)/,
  pendenciaXr: /me (envia|encaminha|manda).{0,30}(resultado|exame|laudo)|aguardo (o |os )?(resultado|exame|retorno)|fico no aguardo|tem previsao para os resultados|precisamos do resultado/,
  planoNegado: /nao esta liberad|nao liberou|nao cobre|carencia|nao atendemos (esse|este|pelo) plano|nao e(sta)? credenciad/,
  dataProposta: /seria viavel|pode ser ?\?|qual (dia|horario) (seria|fica) melhor|temos (disponibilidade|horario|vaga)|consigo (encaixar|agendar)|podemos agendar/,
};

function sinais(db, c) {
  const msgs = db.prepare("SELECT direcao, texto FROM mensagens WHERE contato_id = ? AND created_at >= ? ORDER BY id DESC LIMIT 60")
    .all(c.id, c.orcamento_enviado_at || '')
    .filter(m => !(m.direcao === 'out' && (String(m.texto).includes(ATHENA_MARCA) || String(m.texto).includes(SAUDACAO_MARCA))));
  const ins = msgs.filter(m => m.direcao === 'in').map(m => normalizar(m.texto)).join(' \n ');
  const outs = msgs.filter(m => m.direcao === 'out').map(m => normalizar(m.texto));
  const ultimaOut = outs[0] || '';
  const tel = String(c.telefone || '').replace(/\D/g, '');
  return {
    pensar: RE.pensar.test(ins),
    pagamento: RE.pagamento.test(ins),
    pendencia: RE.pendenciaCli.test(ins) || RE.pendenciaXr.test(ultimaOut),
    plano: outs.some(o => RE.planoNegado.test(o)),
    dataProposta: RE.dataProposta.test(ultimaOut),
    fora: tel.startsWith('55') && (tel.length === 12 || tel.length === 13) && tel.slice(2, 4) !== '31',
    clienteRespondeu: ins.trim().length > 0,
  };
}

// ─── Texto sugerido ────────────────────────────────────────────────────────────
const NAO_NOME = /^(clinica|clinic|vet|veterinari|dra?|dr|doutor|pet|petshop|hospital|centro|lab|laborator|atendimento|comercial|financeiro|recepcao|contato|whatsapp|loja|sr|sra)$/;
function primeiroNome(nome) {
  const w = String(nome || '').trim().split(/\s+/)[0] || '';
  const limpo = w.replace(/[^A-Za-zÀ-ÿ]/g, '');
  if (limpo.length < 3 || limpo.length > 14 || limpo !== w) return '';
  if (NAO_NOME.test(normalizar(limpo))) return '';
  return limpo.charAt(0).toUpperCase() + limpo.slice(1).toLowerCase();
}
function nomePet(pet) {
  const p = String(pet || '').trim();
  if (!p || p.length > 20 || /^animal$/i.test(p) || /\d/.test(p)) return '';
  return p.charAt(0).toUpperCase() + p.slice(1);
}
const EXAME = { TC: 'tomografia', RM: 'ressonância', RX: 'raio-X', US: 'ultrassom' };
const COM_ANESTESIA = { TC: true, RM: true };

function sugerir(db, c, agora = new Date()) {
  const e = estado(db, c, agora);
  const s = sinais(db, c);
  const h = agora.getHours();
  const saud = h < 12 ? 'Bom dia' : h < 18 ? 'Boa tarde' : 'Boa noite';
  const nome = primeiroNome(c.nome);
  const pet = nomePet(c.pet_nome);
  const mod = c.orcamento_modalidade || '';
  const exame = EXAME[mod] || 'exame';
  const art = (exame === 'raio-X' || exame === 'ultrassom' || exame === 'exame') ? 'o' : 'a';       // "a tomografia", "o ultrassom"
  // Sem artigo de gênero: "de Suzy", "Como está Thor?". Adivinhar "do/da" pelo final do nome errava (Suzy, Mel, Nicole).
  const dePet = pet ? 'de ' + pet : 'do seu pet';
  const oPet = pet ? pet : 'o seu pet';
  const ola = `${saud}${nome ? ', ' + nome : ''}! Tudo bem?`;
  const cond = REGRAS.condicaoPagamento;
  const anest = !!COM_ANESTESIA[mod];
  const fora = s.fora ? ' Como vocês vêm de fora, dá para fazer tudo em uma viagem só.' : '';
  let motivo, texto;

  if (e.etapa === 1) {
    if (s.pendencia) {
      motivo = 'aguardando exames';
      texto = `${ola} Os exames ${dePet} já ficaram prontos? Assim que tiver os resultados, me envia por aqui que já deixo ${art} ${exame} agendad${art}.` +
        (anest ? ` Se for mais fácil, fazemos o exame de sangue e o eletro aqui na XR, no mesmo dia d${art} ${exame}.` : '') + fora;
    } else if (s.plano) {
      motivo = 'plano não liberou';
      texto = `${ola} Como o plano não liberou ${art} ${exame} ${dePet}, deixei separada a condição no particular: conseguimos ${cond}. Tenho [dia e hora] disponível. Quer que eu reserve?` + fora;
    } else if (s.dataProposta) {
      motivo = 'data proposta sem confirmação';
      texto = `${ola} Ainda consigo segurar um horário para ${art} ${exame} ${dePet}: [dia e hora]. Posso confirmar para você?` + fora;
    } else {
      motivo = 'orçamento sem resposta';
      texto = `${ola} Passando para saber se ficou alguma dúvida sobre o orçamento d${art} ${exame} ${dePet}. Tenho [dia e hora] disponível` +
        (anest ? ' e conseguimos fazer os exames pré-anestésicos aqui, no mesmo dia.' : '.') + ' Posso reservar para você?' + fora;
    }
  } else if (e.etapa === 2) {
    const como = `${saud}${nome ? ', ' + nome : ''}! Como está ${oPet}?`;
    if (s.pendencia) {
      motivo = 'aguardando exames';
      texto = `${como} Conseguiu fazer os exames?` + (anest ? ` Se ajudar, fazemos tudo aqui no mesmo dia d${art} ${exame}: o resultado do sangue sai na hora.` : '') + ' Me diz o melhor dia que eu encaixo.' + fora;
    } else if (s.pagamento) {
      motivo = 'falou de valor ou pagamento';
      texto = `${como} Sobre o valor d${art} ${exame}: conseguimos ${cond}.` + (anest ? ' Se já tiver exame de sangue ou eletro feitos em outro lugar, aceitamos também.' : '') + ' Quer que eu veja um horário para esta semana?' + fora;
    } else {
      motivo = s.pensar ? 'ficou de retornar' : 'segue sem resposta';
      texto = `${como} Se o que pesou foi o valor, conseguimos ${cond}. Se foi a data, me diz o melhor dia e período que eu encaixo.` + fora;
    }
  } else {
    motivo = 'última mensagem';
    texto = `${saud}${nome ? ', ' + nome : ''}! Vou encerrar por aqui o orçamento d${art} ${exame} ${dePet} para não incomodar. Se o veterinário mantiver a indicação, é só me chamar: seguro as mesmas condições por mais ${REGRAS.validadeDias} dias.`;
  }

  const rotulo = e.etapa === 1 ? '1ª mensagem' : e.etapa === 2 ? '2ª mensagem' : 'Última mensagem';
  return { etapa: e.etapa, feitas: e.feitas, rotulo, motivo, texto, preencher: texto.includes('['), sinais: s };
}

// ─── Coluna vermelha: cliente falou por último e ficou sem resposta ────────────
// Recente (menos de N horas): não há texto pronto, porque o que falta é RESPONDER a pergunta dele.
// Antigo (ficou de um dia para o outro, ou mais): sugere retomar pedindo desculpas pela demora.
// O colchete obriga quem envia a ler a conversa e completar com a resposta de verdade.
const HORAS_VERMELHO_COM_SUGESTAO = 20;
const RE_QUER_AGENDAR = /pode (agendar|marcar|confirmar)|quero (agendar|marcar|fazer)|vamos (agendar|marcar)|vou fazer sim|pode ser sim|fechado/;

function sugerirVermelho(db, c, agora = new Date()) {
  const ult = db.prepare("SELECT texto, created_at FROM mensagens WHERE contato_id = ? AND direcao = 'in' ORDER BY id DESC LIMIT 3").all(c.id);
  if (!ult.length) return null;
  const quando = dt(ult[0].created_at);
  if (!quando || (agora - quando) < HORAS_VERMELHO_COM_SUGESTAO * 3600000) return null;
  const h = agora.getHours();
  const saud = h < 12 ? 'Bom dia' : h < 18 ? 'Boa tarde' : 'Boa noite';
  const nome = primeiroNome(c.nome);
  const pet = nomePet(c.pet_nome);
  const exame = EXAME[c.orcamento_modalidade || ''] || 'exame';
  const art = (exame === 'raio-X' || exame === 'ultrassom' || exame === 'exame') ? 'o' : 'a';
  const dePet = pet ? 'de ' + pet : 'do seu pet';
  const ola = `${saud}${nome ? ', ' + nome : ''}! Desculpe a demora no nosso retorno.`;
  const falas = normalizar(ult.map(m => m.texto).join(' '));
  let motivo, texto;
  if (RE_QUER_AGENDAR.test(falas)) {
    motivo = 'cliente pediu para agendar e ficou sem resposta';
    texto = `${ola} Vamos agendar sim ${art} ${exame} ${dePet}. Tenho [dia e hora] disponível. Fica bom para você?`;
  } else {
    motivo = 'ficou sem resposta';
    texto = `${ola} [responder aqui o que o cliente perguntou] Ainda posso ajudar com ${art} ${exame} ${dePet}?`;
  }
  return { etapa: 0, feitas: 0, rotulo: 'Retomar com desculpas', motivo, texto, preencher: true };
}

// ─── Resultado (para a aba Métricas) ───────────────────────────────────────────
// Por contato retomado no período: respondeu depois? agendou depois?
function resumo(db, desde, agendouDepois) {
  const evs = db.prepare("SELECT contato_id, valor, created_at FROM followup_eventos WHERE tipo = 'retomada' AND created_at >= ? ORDER BY id").all(desde);
  const porContato = {};
  const porEtapa = { 1: 0, 2: 0, 3: 0 };
  for (const e of evs) {
    porEtapa[e.valor] = (porEtapa[e.valor] || 0) + 1;
    if (!porContato[e.contato_id]) porContato[e.contato_id] = e.created_at;   // 1ª retomada do contato no período
  }
  const respondeu = db.prepare("SELECT 1 FROM mensagens WHERE contato_id = ? AND direcao = 'in' AND created_at > ? LIMIT 1");
  let responderam = 0, agendaram = 0;
  for (const [id, t] of Object.entries(porContato)) {
    if (respondeu.get(Number(id), t)) responderam++;
    const ag = agendouDepois ? agendouDepois(Number(id), t) : null;
    if (ag) agendaram++;
  }
  const contatos = Object.keys(porContato).length;
  const pct = (a, b) => b ? Math.round(1000 * a / b) / 10 : 0;
  return { mensagens: evs.length, contatos, responderam, agendaram, taxa_resposta: pct(responderam, contatos), taxa_agendou: pct(agendaram, contatos), por_etapa: porEtapa };
}

module.exports = { REGRAS, RE_AGENDOU, estado, inicioDoCiclo, registrarSeRetomada, sinais, sugerir, sugerirVermelho, primeiroNome, nomePet, resumo };
