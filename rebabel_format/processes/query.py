#!/usr/bin/env python3

from .process import Process
from .parameters import QueryParameter

class Search(Process):
    '''Find instances of a pattern in the corpus'''

    name = 'query'
    query = QueryParameter()

    def run(self):
        from ..config import parse_feature
        from collections import defaultdict
        from ..query import search
        print_feats = defaultdict(list)
        lab_width = 0
        for name, item in self.query.items():
            if not isinstance(item, dict):
                continue
            pr = item.get('print', [])
            typ = item.get('type')
            if typ is None:
                continue
            for p in pr:
                t, f = parse_feature(p)
                fid, _ = self.db.get_feature(typ, t, f, error=True)
                lab = f'{t}:{f}'
                lab_width = max(lab_width, len(lab))
                print_feats[name].append((lab, fid))
        for n, result in enumerate(search(self.db, self.query), 1):
            print('Result', n)
            for name, uid in sorted(result.items()):
                print(name, uid)
                for lab, fid in print_feats[name]:
                    v = str(self.db.get_feature_value(uid, fid))
                    print('\t'+lab.ljust(lab_width+3)+'\t'+v)
            print('')
