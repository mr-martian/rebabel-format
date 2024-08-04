#!/usr/bin/env python3

from .process import SearchProcess
from .parameters import Parameter, FeatureParameter
from collections import Counter, defaultdict

class Distribution(SearchProcess):
    name = 'distribution'
    center = Parameter(default='Center')
    child_type = Parameter(type=str)
    child_print = Parameter(type=list)
    sort = FeatureParameter(default='meta:index')
    include = Parameter(default=[], type=list)

    def pre_search(self):
        self.counter = Counter()
        child_features = set()
        for block in self.child_print:
            block['fid'] = self.get_feature(self.child_type, block['feature'])[0]
            child_features.add(block['fid'])
        self.sort_feature = self.db.get_feature(self.child_type, *self.sort)[0]
        child_features.add(self.sort_feature)
        self.child_features = list(child_features)
        self.parents = defaultdict(list)

    def display_unit(self, features):
        pieces = []
        for block in self.child_print:
            val = features.get(block['fid'], '_')
            if 'exclude' in block and val in block['exclude']:
                return '_'
            elif 'include' in block and val not in block['include']:
                return '_'
            pieces.append(str(val))
        return '/'.join(pieces)

    def per_result(self, result):
        lab = [str(self.get_value(result, i)) for i in self.include]
        self.parents[result[self.center]].append(lab)

    def post_search(self):
        dct = self.db.get_children(list(self.parents.keys()), self.child_type)
        for parent, labs in self.parents.items():
            children = [self.display_unit(f) for f in
                        sorted(
                            [self.db.get_unit_features(c, self.child_features)
                             for c in dct[parent]],
                            key=lambda f: f.get(self.sort_feature, 0),
                        )]
            for lab in labs:
                self.counter['\t'.join(lab + children)] += 1
        cols = ['Count'] + [x['feature'] for x in self.include] + ['Items']
        print('\t'.join(cols))
        for pattern, count in self.counter.most_common():
            print(f'{count}\t{pattern}')
