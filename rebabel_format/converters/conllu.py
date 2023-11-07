#!/usr/bin/env python3

from .reader import Reader
from .writer import Writer

# General TODO: this currently assumes valid input and crashes randomly on errors

class ConnluReader(Reader):
    identifier = 'conllu'
    short_name = 'CoNNL-U'
    long_name = 'Universal Dependencies CoNNL-U format'

    def read_file(self, fin):
        self.ensure_feature('word', 'UD', 'index', 'int')
        self.ensure_feature('token', 'UD', 'index', 'int')
        cur = []
        for line in fin:
            if not line.strip():
                self.process_sentence(cur)
                cur = []
            else:
                cur.append(line.rstrip())
        if cur:
            self.process_sentence(cur)

    def set_feats(self, wid: int, wtype: str, featstr: str, misc: bool):
        if featstr == '_':
            return
        tier = 'UD/MISC' if misc else 'UD/FEATS'
        for fs in featstr.split('|'):
            name, val = fs.split('=', 1)
            typ = 'str'
            if misc and name == 'SpaceAfter':
                typ = 'bool'
                val = (val != 'No')
            self.ensure_feature(wtype, tier, name, typ)
            self.set_feature(wid, tier, name, val)

    def process_sentence(self, lines):
        sent_id = self.create_unit('sentence')
        idx = 0
        words = {}
        for l in lines:
            if l.startswith('#'):
                key, val = l[1:].split('=', 1)
                key = key.strip()
                val = val.strip()
                self.ensure_feature('sentence', 'UD', key, 'str')
                self.set_feature(sent_id, 'UD', key, val)
            else:
                ls = l.split('\t')
                typ = 'token' if '-' in ls[0] else 'word'
                wid = self.create_unit(typ, parent=sent_id)
                self.set_feature(wid, 'UD', 'index', idx)
                idx += 1
                if '.' in ls[0]:
                    self.ensure_feature('word', 'UD', 'null', 'bool')
                    self.set_feature(wid, 'UD', 'null', True)
                self.set_feats(wid, typ, ls[5], False)
                self.set_feats(wid, typ, ls[9], True)
                cols = [('form', 1), ('lemma', 2), ('upos', 3), ('xpos', 4),
                        ('deprel', 7)]
                for n, i in cols:
                    if ls[i] != '_':
                        self.ensure_feature(typ, 'UD', n, 'str')
                        self.set_feature(wid, 'UD', n, ls[i])
                if typ == 'word':
                    words[ls[0]] = (wid, ls[6], ls[8])
        for wid, head, deps in words.values():
            if head in words:
                hid = words[head][0]
                self.ensure_feature('word', 'UD', 'head', 'ref')
                self.set_feature(wid, 'UD', 'head', hid)
            if deps != '_':
                for link in deps.split('|'):
                    h, r = link.split(':', 1)
                    if h in words:
                        hid = words[head][0]
                        lid = self.create_unit('UD-edep', parent=sent_id)
                        self.ensure_feature('UD-edep', 'UD', 'parent', 'ref')
                        self.ensure_feature('UD-edep', 'UD', 'child', 'ref')
                        self.ensure_feature('UD-edep', 'UD', 'label', 'str')
                        self.set_feature(lid, 'UD', 'parent', hid)
                        self.set_feature(lid, 'UD', 'child', wid)
                        self.set_feature(lid, 'UD', 'label', r)
        self.commit()

class ConlluWriter(Writer):
    identifier = 'conllu'

    def write(self, fout):
        # TODO: sorting (meta:index?)
        feat_dict = self.all_feats_from_tiers(['UD', 'UD/FEATS', 'UD/MISC'])
        feat_list = list(feat_dict.keys())
        sent_id = None
        null = None
        for k, v in feat_dict.items():
            if v == ('UD', 'sent_id', 'str') or v == ('UD', 'sent_id', 'int'):
                sent_id = k
            elif v == ('UD', 'null', 'bool'):
                null = k
        for sid, sfeats in self.iter_units('sentence', feat_list):
            si = sfeats.get(sent_id, sid)
            fout.write(f'# sent_id = {si}\n')
            for k, v in sfeats.items():
                if k == sent_id:
                    continue
                tier, feat, typ = feat_dict[k]
                if tier == 'UD':
                    fout.write(f'# {feat} = {v}\n')
            words = []
            id2idx = {}
            idx1 = 0
            idx2 = 0
            heads = [] # [(words index, id), ...]
            for wid, wfeats in self.iter_units('word', feat_list, sid):
                widx = ''
                if wfeats.get(null, False) == True:
                    idx2 += 1
                    widx = f'{idx1}:{idx2}'
                else:
                    idx1 += 1
                    idx2 = 0
                    widx = str(idx1)
                id2idx[wid] = widx
                row = [widx] + ['_']*9
                FEAT = []
                MISC = []
                for i, v in wfeats.items():
                    tier, feat, typ = feat_dict[i]
                    if tier == 'UD/FEATS':
                        FEAT.append(f'{feat}={v}')
                    elif tier == 'UD/MISC':
                        MISC.append(f'{feat}={v}')
                    elif tier == 'UD':
                        if feat == 'form':
                            row[1] = v
                        elif feat == 'lemma':
                            row[2] = v
                        elif feat == 'upos':
                            row[3] = v
                        elif feat == 'xpos':
                            row[4] = v
                        elif feat == 'deprel':
                            row[7] = v
                        elif feat == 'head':
                            heads.append((len(words), v))
                if FEAT:
                    row[5] = '|'.join(sorted(FEAT))
                if MISC:
                    row[9] = '|'.join(sorted(MISC))
                words.append(row)
            for i, h in heads:
                if h in id2idx:
                    words[i][6] = id2idx[h]
            # TODO: tokens
            fout.write('\n'.join('\t'.join(row) for row in words) + '\n\n')
