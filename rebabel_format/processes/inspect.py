#!/usr/bin/env python3

from rebabel_format.process import Process
from rebabel_format.parameters import Parameter
from collections import defaultdict

class Inspect(Process):
    name = 'inspect'
    schema = Parameter(type=bool, default=False, help='whether to output the defined features for all unit types')

    def run(self):
        if self.schema:
            feats = self.db.get_all_features()
            fd = defaultdict(lambda: defaultdict(list))
            for fid, tier, name, utype, vtype in feats:
                fd[utype][tier].append((name, vtype))
            for utype, d1 in sorted(fd.items()):
                print(utype)
                for tier, d2 in sorted(d1.items()):
                    if tier == 'meta' and len(d2) == 1:
                        continue
                    print('\t' + tier)
                    for feat, vtype in sorted(d2):
                        if tier == 'meta' and feat == 'active':
                            continue
                        print(f'\t\t{feat}: {vtype}')
                print('')
