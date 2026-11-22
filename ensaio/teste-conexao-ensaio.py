# -*- coding: utf-8 -*-
"""Ensaio da conexão/sincronização do CRM contra a cópia _ensaio-crm (porta 8196) e o WhatsApp de mentira.
Uso: python3 teste-conexao-ensaio.py http://172.18.0.3:8196
"""
import sys, json, time, urllib.request, datetime, random

BASE = sys.argv[1].rstrip('/')
TEL = '5500000000555'  # número de ensaio (não existe)
falhas = []

def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(r, timeout=60) as resp:
        return json.loads(resp.read().decode() or '{}')

def ok(cond, msg, extra=''):
    print(('  OK   ' if cond else '  FALHA') + ' ' + msg + (' -> ' + str(extra)[:300] if extra != '' else ''))
    if not cond: falhas.append(msg)

def ts(dias, hora, minuto):
    sp = datetime.timezone(datetime.timedelta(hours=-3))
    d = datetime.datetime.now(sp) - datetime.timedelta(days=dias)
    d = d.replace(hour=hora, minute=minuto, second=0, microsecond=0)
    return int(d.timestamp()), d.strftime('%Y-%m-%d %H:%M:%S')

def msg(i, from_me, texto, t, tipo='text'):
    m = {'key': {'remoteJid': TEL + '@s.whatsapp.net', 'fromMe': from_me, 'id': 'ENSAIO-HIST-%d-%d' % (t, i)},
         'pushName': 'Voce' if from_me else 'Tutor Ensaio Sinc', 'messageTimestamp': t, 'source': 'android', 'status': 'DELIVERY_ACK'}
    if tipo == 'image':
        m['message'] = {'imageMessage': {'caption': texto, 'mimetype': 'image/jpeg', 'url': 'https://x/y.enc'}}
        m['messageType'] = 'imageMessage'
    else:
        m['message'] = {'conversation': texto}
        m['messageType'] = 'conversation'
    return m

print('1) estado')
e = req('GET', '/api/whatsapp/estado')
ok(e.get('estado') == 'close', 'instância fechada no ensaio', e.get('estado'))
ok(e.get('motivo') == 401 or e.get('motivo') == '401', 'motivo 401 vindo da lista de instâncias', e.get('motivo'))
ok(bool(e.get('ultimaRecebida')), 'última recebida preenchida', e.get('ultimaRecebida'))
ok(e.get('numero') == '553182043117', 'número da recepção', e.get('numero'))

print('2) QR e código')
q = req('GET', '/api/whatsapp/qr')
ok(q.get('conectado') is False and str(q.get('base64', '')).startswith('data:image'), 'QR em base64', str(q.get('base64', ''))[:30])
q2 = req('GET', '/api/whatsapp/qr?numero=553182043117')
ok(bool(q2.get('pairingCode')), 'código por número', q2.get('pairingCode'))

print('3) histórico ao reconectar (messages.set, fora de ordem)')
t1, s1 = ts(3, 10, 0); t2, s2 = ts(3, 10, 5); t3, s3 = ts(3, 10, 7); t4, s4 = ts(3, 11, 0); t5, s5 = ts(3, 11, 1); tv, sv = ts(20, 9, 0)
lote = [
    msg(1, False, 'Oi, quero orçamento de tomografia (ensaio)', t1),
    msg(2, True, 'Bom dia! Segue o orçamento (ensaio)', t2),
    msg(3, False, 'Obrigada (ensaio)', t3),
    msg(4, False, 'Qual o valor com anestesia? (ensaio)', t4),
    msg(5, False, 'foto do pedido (ensaio)', t5, 'image'),
    msg(6, False, 'mensagem velha de 20 dias (ensaio)', tv),
]
random.shuffle(lote)
r = req('POST', '/webhook/evolution', {'event': 'messages.set', 'instance': 'ensaio', 'data': lote, 'extra': {'isLatest': True, 'progress': 100}})
ok(r.get('historico') == 6, 'lote aceito (6)', r)
for _ in range(30):
    time.sleep(1)
    sy = req('GET', '/api/whatsapp/estado').get('sync', {})
    if not sy.get('emAndamento') and sy.get('lotes'): break
ok(sy.get('gravadas') == 5, '5 gravadas', sy)
ok(sy.get('foraDaJanela') == 1, '1 fora da janela (20 dias)', sy.get('foraDaJanela'))
ok(sy.get('ignoradas') == 0 and sy.get('erros') == 0, 'nenhuma ignorada/erro', (sy.get('ignoradas'), sy.get('erros'), sy.get('ultimoErro')))
ok(sy.get('conversas') == 1, '1 conversa', sy.get('conversas'))

busca = req('GET', '/api/contatos/buscar?q=' + TEL)
lista = busca if isinstance(busca, list) else busca.get('contatos') or busca.get('resultados') or []
cid = (lista[0] or {}).get('id') if lista else None
ok(bool(cid), 'contato de ensaio criado', lista[:1])
c = req('GET', '/api/contato/%s?visto=0' % cid)
ms = sorted(c.get('mensagens', []), key=lambda m: (m['created_at'], m['id']))
ok([m['created_at'] for m in ms] == [s1, s2, s3, s4, s5], 'ordem e hora verdadeiras', [m['created_at'] for m in ms])
ok([m['direcao'] for m in ms] == ['in', 'out', 'in', 'in', 'in'], 'direções', [m['direcao'] for m in ms])
ok(ms[-1]['tipo'] == 'image' and bool(ms[-1].get('media_path')), 'foto do histórico baixada com a mensagem inteira', (ms[-1]['tipo'], ms[-1].get('media_path')))
ok(c.get('last_read_at') == s2, 'lido até a resposta da recepção (%s)' % s2, c.get('last_read_at'))
ok(c.get('last_message_at') == s5, 'última mensagem = hora verdadeira da última', c.get('last_message_at'))
conv = req('GET', '/api/contatos/conversas?q=' + TEL)
lconv = conv if isinstance(conv, list) else conv.get('contatos') or conv.get('conversas') or []
me = [x for x in lconv if str(x.get('telefone')) == TEL]
campos = {k: v for k, v in (me[0] if me else {}).items() if 'lid' in k.lower() or 'unread' in k.lower() or 'nao' in k.lower()}
ok(bool(me), 'aparece na lista de conversas', campos)

print('4) o mesmo lote de novo: nada duplica')
r = req('POST', '/webhook/evolution', {'event': 'MESSAGES_SET', 'instance': 'ensaio', 'data': lote})
for _ in range(20):
    time.sleep(1)
    sy = req('GET', '/api/whatsapp/estado').get('sync', {})
    if not sy.get('emAndamento') and sy.get('lotes') == 2: break
ok(sy.get('gravadas') == 5 and sy.get('ignoradas') == 5, 'repetição ignorada', (sy.get('gravadas'), sy.get('ignoradas')))
c2 = req('GET', '/api/contato/%s?visto=0' % cid)
ok(len(c2.get('mensagens', [])) == 5, 'continua com 5 mensagens', len(c2.get('mensagens', [])))

print('5) mensagem ao vivo depois do histórico')
agora = int(time.time())
r = req('POST', '/webhook/evolution', {'event': 'messages.upsert', 'instance': 'ensaio', 'data': msg(9, False, 'chegou agora, ao vivo (ensaio)', agora)})
ok(r.get('ok') and r.get('gravada'), 'upsert ao vivo gravado', r)
c3 = req('GET', '/api/contato/%s?visto=0' % cid)
ms3 = sorted(c3.get('mensagens', []), key=lambda m: (m['created_at'], m['id']))
ok(len(ms3) == 6 and ms3[-1]['texto'].startswith('chegou agora'), 'fica por último', ms3[-1]['texto'])
ok(c3.get('last_message_at', '') >= ms3[-1]['created_at'][:16], 'last_message_at andou para a frente', c3.get('last_message_at'))
ok(c3.get('last_read_at') == s2, 'continua não lida desde a resposta', c3.get('last_read_at'))

print('6) connection.update')
r = req('POST', '/webhook/evolution', {'event': 'connection.update', 'instance': 'ensaio', 'data': {'state': 'open', 'statusReason': 200}})
e2 = req('GET', '/api/whatsapp/estado')
ok(e2.get('conexao', {}).get('estado') == 'open', 'evento de conexão registrado', e2.get('conexao'))

print('7) comando de follow-up no histórico não executa')
r = req('POST', '/webhook/evolution', {'event': 'messages.set', 'instance': 'ensaio', 'data': [
    {'key': {'remoteJid': '553183746380@s.whatsapp.net', 'fromMe': True, 'id': 'ENSAIO-CMD-%d' % agora}, 'pushName': 'Voce', 'messageTimestamp': agora - 60, 'message': {'conversation': 'ok 1'}, 'messageType': 'conversation'}]})
time.sleep(2)
sy = req('GET', '/api/whatsapp/estado').get('sync', {})
ok(sy.get('erros') == 0, 'sem erro no lote do comando', sy.get('ultimoErro'))

print()
print('FALHAS:', len(falhas)) if falhas else print('TUDO OK')
for f in falhas: print(' -', f)
sys.exit(1 if falhas else 0)
