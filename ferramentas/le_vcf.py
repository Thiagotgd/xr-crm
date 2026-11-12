#!/usr/bin/env python3
"""Lê um arquivo .vcf (agenda exportada do celular) e gera a lista de contatos: nome + telefones (só dígitos).
Uso: python -I le_vcf.py <arquivo.vcf> [saida.json]. Sem saída, só mostra o resumo (sem nomes)."""
import json
import quopri
import re
import sys
from collections import Counter


def abre(caminho):
    bruto = open(caminho, 'rb').read()
    for cod in ('utf-8-sig', 'utf-16', 'latin-1'):
        try:
            return bruto.decode(cod)
        except UnicodeError:
            continue
    return bruto.decode('latin-1', 'replace')


def desdobra(texto):
    # linhas continuadas (começam com espaço/tab) e quoted-printable com "=" no fim
    linhas = []
    for l in texto.replace('\r\n', '\n').replace('\r', '\n').split('\n'):
        if linhas and (l.startswith(' ') or l.startswith('\t')):
            linhas[-1] += l[1:]
        elif linhas and linhas[-1].endswith('=') and 'QUOTED-PRINTABLE' in linhas[-1].split(':', 1)[0].upper():
            linhas[-1] = linhas[-1][:-1] + l
        else:
            linhas.append(l)
    return linhas


def valor(linha):
    cab, _, v = linha.partition(':')
    if 'QUOTED-PRINTABLE' in cab.upper():
        cod = (re.search(r'CHARSET=([\w-]+)', cab, re.I) or [None, 'utf-8'])[1]
        try:
            v = quopri.decodestring(v.encode('latin-1', 'replace')).decode(cod, 'replace')
        except Exception:
            pass
    return v.replace('\\,', ',').replace('\\;', ';').replace('\\n', ' ').replace('\\N', ' ').strip()


def contatos(caminho):
    saida, atual = [], None
    for l in desdobra(abre(caminho)):
        u = l.upper()
        if u.startswith('BEGIN:VCARD'):
            atual = {'fn': '', 'n': '', 'org': '', 'tels': []}
        elif u.startswith('END:VCARD'):
            if atual is not None:
                saida.append(atual)
            atual = None
        elif atual is None:
            continue
        else:
            campo = re.split(r'[;:]', l, maxsplit=1)[0].upper().split('.')[-1]
            if campo == 'FN':
                atual['fn'] = valor(l)
            elif campo == 'N':
                p = [x.strip() for x in valor(l).split(';')]
                atual['n'] = ' '.join(x for x in [p[3] if len(p) > 3 else '', p[1] if len(p) > 1 else '', p[2] if len(p) > 2 else '', p[0] if p else ''] if x)
            elif campo == 'ORG':
                atual['org'] = valor(l).replace(';', ' ').strip()
            elif campo == 'TEL':
                waid = re.search(r'waid=(\d+)', l, re.I)
                d = waid.group(1) if waid else re.sub(r'\D', '', valor(l))
                if d:
                    atual['tels'].append(d)
    lista = []
    for c in saida:
        nome = re.sub(r'\s+', ' ', c['fn'] or c['n'] or c['org']).strip()
        tels = list(dict.fromkeys(c['tels']))
        lista.append({'nome': nome, 'tels': tels})
    return lista


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    lista = contatos(sys.argv[1])
    com_tel = [c for c in lista if c['tels']]
    print('cartões no arquivo:', len(lista), '| com telefone:', len(com_tel), '| sem nome:', sum(1 for c in com_tel if not c['nome']),
          '| telefones no total:', sum(len(c['tels']) for c in com_tel), '| com mais de um telefone:', sum(1 for c in com_tel if len(c['tels']) > 1))
    print('tamanho dos números (dígitos → quantos):', dict(sorted(Counter(len(t) for c in com_tel for t in c['tels']).items())))
    print('começam com 55:', sum(1 for c in com_tel for t in c['tels'] if t.startswith('55') and len(t) >= 12), '| começam com 0:', sum(1 for c in com_tel for t in c['tels'] if t.startswith('0')))
    rep = Counter(t for c in com_tel for t in c['tels'])
    print('telefones repetidos em mais de um cartão:', sum(1 for t, n in rep.items() if n > 1))
    print('tamanho dos nomes (letras): mín', min((len(c['nome']) for c in com_tel if c['nome']), default=0), 'máx', max((len(c['nome']) for c in com_tel), default=0))
    if len(sys.argv) > 2:
        json.dump(com_tel, open(sys.argv[2], 'w', encoding='utf-8'), ensure_ascii=False)
        print('gravado em', sys.argv[2])
