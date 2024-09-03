#!/usr/bin/env python3

from rebabel_format.reader import LineReader, ReaderError
from rebabel_format.writer import Writer

class ConnluReader(LineReader):
    '''
    Units imported:

    - sentence

    Each sentence is imported as a top-level unit. Any comment lines which
    contain `=` will be imported as string features in the `UD` tier.

    - word, token

    Each line of the sentence will be added as either a word node or a token
    node. The parent will be set to the sentence and relations will be added
    from any overt word to the containing token.

    The non-empty single-valued columns will be imported in the `UD` tier with
    the names `id`, `form`, `lemma`, `upos`, `xpos`, `head`, and `deprel`.
    All of these are string features except `head`, which is a reference feature.
    Words whose head is `0` will have `UD:head` unset.

    The features and miscelaneous columns will be imported as string features
    in the tiers `UD/FEATS` and `UD/MISC`, respectively.
    An exception is made for `UD/MISC:SpaceAfter`, which is imported as a boolean
    feature.

    - UD-edep

    Enhanced dependencies are imported as `UD-edep` nodes. Their parent is the
    sentence and they have reference features `UD:parent` and `UD:child` and
    a string feature `UD:deprel`.
    '''

    identifier = 'conllu'
    short_name = 'CoNNL-U'
    long_name = 'Universal Dependencies CoNNL-U format'
    format_specification = 'https://universaldependencies.org/format'

    block_name = 'sentence'

    def reset(self):
        self.word_idx = 0
        self.token_idx = 0
        self.edep_count = 0

    def end(self):
        if self.id_seq:
            self.set_type('sentence', 'sentence')
            self.set_feature('sentence', 'meta', 'index', 'int',
                             self.block_count)
        super().end()

    def process_line(self, line):
        if not line:
            return

        if line[0] == '#':
            if '=' in line:
                k, v = line[1:].split('=', 1)
                self.set_feature('sentence', 'UD', k.strip(), 'str', v.strip())
            return

        columns = line.strip().split('\t')
        if len(columns) != 10:
            self.error(f'Expected 10 columns, found {len(columns)}.')

        name = columns[0]
        is_token = '-' in name
        self.set_type(name, 'token' if is_token else 'word')
        self.set_parent(name, 'sentence')
        self.set_feature(name, 'UD', 'id', 'str', name)
        if is_token:
            self.token_idx += 1
            self.set_feature(name, 'meta', 'index', 'int', self.token_idx)
            a, b = name.split('-', 1)
            if a.isdigit() and b.isdigit():
                for ch in range(int(a), int(b)+1):
                    self.add_relation(str(ch), name)
        else:
            self.word_idx += 1
            self.set_feature(name, 'meta', 'index', 'int', self.word_idx)
            self.set_feature(name, 'UD', 'null', 'bool', '.' in name)

        for group, val in [('UD/FEATS', columns[5]), ('UD/MISC', columns[9])]:
            if val == '_':
                continue
            for pair in val.split('|'):
                if '=' not in pair:
                    self.error(f"Invalid key-value pair '{pair}'.")
                k, v = pair.split('=', 1)
                typ = 'str'
                if k == 'SpaceAfter':
                    typ = 'bool'
                    v = (v != 'No')
                self.set_feature(name, group, k, typ, v)

        col_names = [('form', 1), ('lemma', 2), ('upos', 3), ('xpos', 4),
                     ('head', 6), ('deprel', 7)]
        for feat, col in col_names:
            if columns[col] != '_':
                typ = 'str'
                if feat == 'head':
                    if columns[col] == '0':
                        continue
                    typ = 'ref'
                self.set_feature(name, 'UD', feat, typ, columns[col])

        if columns[8] != '_':
            for pair in columns[8].split('|'):
                if ':' not in pair:
                    self.error(f"Invalid dependency specifier '{pair}'.")
                self.edep_count += 1
                ename = f'\t{self.edep_count}'
                self.set_type(ename, 'UD-edep')
                self.set_parent(ename, 'sentence')
                head, rel = pair.split(':', 1)
                self.set_feature(ename, 'UD', 'parent', 'ref', h)
                self.set_feature(ename, 'UD', 'child', 'ref', name)
                self.set_feature(ename, 'UD', 'deprel', 'str', rel)

class ConlluWriter(Writer):
    identifier = 'conllu'

    query_order = ['sentence', 'word']
    query = {
        'sentence': {
            'type': 'sentence',
            'order': 'meta:index',
        },
        'word': {
            'type': ['word', 'token', 'UD-edep'],
            'parent': 'sentence',
        },
    }

    def write(self, fout):
        feat2col = {'form': 1, 'lemma': 2, 'upos': 3, 'xpos': 4, 'deprel': 7}
        sent_feats = self.table.add_tier('sentence', 'UD', prefix=False)
        word_feats = self.table.add_tier('word', 'UD', prefix=True)
        sent_id = sent_feats.get(('UD', 'sent_id'))
        sent_feat_ids = sorted([(f, i) for (t, f), i in sent_feats.items()
                                if f != 'sent_id'])
        word_feat_ids = sorted([(t, f, i) for (t, f), i in word_feats.items()])
        import itertools
        for _, word_group in itertools.groupby(self.table.results(),
                                               lambda x: x[0]['sentence']):
            sentence = list(word_group)
            sid = sentence[0][0]['sentence']
            sfeats = sentence[0][1][sid]
            if sent_id in sfeats:
                fout.write(f'# sent_id = {sfeats[sent_id]}\n')
            for feat, fid in sent_feat_ids:
                if fid in sfeats:
                    fout.write(f'# {feat} = {sfeats[fid]}\n')

            table = []
            wid2idx = {}
            wid2num = {}

            word_num = 0
            null_num = 0

            tokens = []
            heads = {}

            for idx, (id_dict, feat_dict) in enumerate(sentence):
                wid = id_dict['word']
                word_type = self.table.get_type(wid)
                if word_type == 'UD-edep':
                    # TODO
                    continue
                line = ['_' for i in range(10)]
                wfeats = feat_dict[wid]
                null = False
                ufeats = []
                umisc = []
                for tier, feat, fid in word_feat_ids:
                    val = wfeats.get(fid)
                    if val is None:
                        continue
                    if tier == 'UD':
                        if feat in feat2col:
                            line[feat2col[feat]] = str(val)
                        elif feat == 'head':
                            heads[wid] = val
                        elif feat == 'null' and val:
                            null = True
                    elif tier == 'UD/FEATS':
                        if isinstance(val, bool):
                            val = 'Yes' if val else 'No'
                        ufeats.append(f'{feat}={val}')
                    elif tier == 'UD/MISC':
                        if isinstance(val, bool):
                            val = 'Yes' if val else 'No'
                        umisc.append(f'{feat}={val}')
                if ufeats:
                    line[5] = '|'.join(ufeats)
                if umisc:
                    line[9] = '|'.join(umisc)
                if word_type == 'word':
                    if null:
                        null_num += 1
                        line[0] = f'{word_num}.{null_num}'
                    else:
                        word_num += 1
                        null_num = 0
                        line[0] = str(word_num)
                    wid2num[wid] = (word_num, null_num)
                    wid2idx[wid] = len(table)
                    table.append(line)
                else:
                    tokens.append((wid, line))

            for dep, head in heads.items():
                if head not in wid2idx:
                    continue
                table[wid2idx[dep]][6] = table[wid2idx[head]][0]
            for i in range(len(table)):
                if table[i][6] == '_' and table[i][7] == 'root':
                    table[i][6] = '0'

            token_rels = self.table.get_relations([t[0] for t in tokens],
                                                  list(wid2num.keys()))
            token_rels.sort()
            for tid, pairs in itertools.groupby(token_rels, key=lambda x: x[0]):
                words = [p[1] for p in pairs]
                nums = []
                for w in words:
                    if w in wid2num:
                        nums.append(wid2num[w][0])
                if nums:
                    # TODO: get word range and insert into table
                    pass

            fout.write('\n'.join('\t'.join(row) for row in table) + '\n\n')
