#!/usr/bin/env python3

from .process import SearchProcess
from .parameters import Parameter, FeatureParameter
from collections import Counter

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
        children = self.db.get_units(self.child_type, result[self.center])
        features = [self.db.get_unit_features(ch, self.child_features)
                    for ch in children]
        features.sort(key=lambda f: f.get(self.sort_feature, 0))
        display = [str(self.get_value(result, i)) for i in self.include]
        display += [self.display_unit(f) for f in features]
        self.counter['\t'.join(display)] += 1

    def post_search(self):
        cols = ['Count'] + [x['feature'] for x in self.include] + ['Items']
        print('\t'.join(cols))
        for pattern, count in self.counter.most_common():
            print(f'{count}\t{pattern}')
