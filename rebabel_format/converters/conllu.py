#!/usr/bin/env python3

from .reader import Reader

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
                if key == 'sent_id': print(val)
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
