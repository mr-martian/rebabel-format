#!/usr/bin/env python3

from rebabel_format.process import Process
from rebabel_format.parameters import Parameter
from collections import defaultdict

class Inspect(Process):
    name = 'inspect'
    schema = Parameter(type=bool, default=False, help='whether to output the defined features for all unit types')

    def add_to_dict(self, pieces, vtype, dct):
        if not pieces:
            dct[None] = vtype
        else:
            if pieces[0] not in dct:
                dct[pieces[0]] = {}
            self.add_to_dict(pieces[1:], vtype, dct[pieces[0]])

    def print_dict(self, dct, depth=1):
        keys = sorted(k for k in dct if k is not None)
        for key in keys:
            if key is None:
                continue
            if None in dct[key]:
                print('\t'*depth + f'{key}: {dct[key][None]}')
            else:
                print('\t'*depth + key)
            self.print_dict(dct[key], depth+1)

    def run(self):
        if self.schema:
            feats = self.db.get_all_features()
            fd = defaultdict(dict)
            for fid, name, utype, vtype in feats:
                if name == 'meta:active':
                    continue
                self.add_to_dict(name.split(':'), vtype, fd[utype])
            for utype, d1 in sorted(fd.items()):
                print(utype)
                self.print_dict(d1)
                print('')
