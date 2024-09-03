#!/usr/bin/env python3

from rebabel_format.process import Process
from rebabel_format.parameters import QueryParameter
from rebabel_format import utils

class Search(Process):
    '''Find instances of a pattern in the corpus'''

    name = 'query'
    query = QueryParameter(help='the pattern to search for')

    def render_unit(self, name, uid):
        print(name, uid)
        for lab, fid in self.print_feats[name]:
            v = str(self.db.get_feature_value(uid, fid))
            if v is None:
                continue
            print('\t'+lab.ljust(self.lab_width+3)+'\t'+v)

    def run(self):
        from rebabel_format.config import parse_feature
        from collections import defaultdict
        from rebabel_format.query import search
        self.print_feats = defaultdict(list)
        self.lab_width = 0
        for name, item in self.query.items():
            if not isinstance(item, dict):
                continue
            pr = item.get('print', [])
            typ = item.get('type')
            if typ is None:
                continue
            for p in pr:
                t, f = parse_feature(p)
                lab = f'{t}:{f}'
                self.lab_width = max(self.lab_width, len(lab))
                got_any = False
                for single_type in utils.as_list(typ):
                    fid, _ = self.db.get_feature(single_type, t, f, error=False)
                    if fid is None: continue
                    self.print_feats[name].append((lab, fid))
                    got_any = True
                if not got_any:
                    raise ValueError(f"Could not find print feature '{p}' for unit '{name}'.")
        for n, result in enumerate(search(self.db, self.query), 1):
            print('Result', n)
            for name, uid in sorted(result.items()):
                for u in utils.as_list(uid):
                    self.render_unit(name, u)
            print('')
