#!/usr/bin/env python3

from rebabel_format.process import Process
from rebabel_format.parameters import Parameter, FeatureParameter, QueryParameter
from rebabel_format.query import ResultTable
from collections import Counter

# TODO: this process seems oddly specific
# perhaps queries should support getting arbitrary numbers of children,
# which would allow this to generalize somewhat

class Distribution(Process):
    '''Generate a frequency list for instances of a pattern'''

    name = 'distribution'
    query = QueryParameter(help='the pattern to search for')
    center = Parameter(default='Center', help='the element of the pattern which is the parent of the units being counted')
    child_type = Parameter(type=str, help='the type of the units being counted')
    child_print = Parameter(type=list, help='the features to count')
    sort = FeatureParameter(default='meta:index', help='the feature to use for sorting')
    include = Parameter(default=[], type=list, help='features from the pattern to include when counting')

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

    def run(self):
        rt = ResultTable(self.db, self.query)
        chname = rt.add_children(self.center, self.child_type)
        printids = rt.add_features(chname,
                                   [cp['feature'] for cp in self.child_print])
        for p, i in zip(self.child_print, printids):
            p['fid'] = i
        sortid = rt.add_features(chname, [self.sort])[0]
        for inc in self.include:
            inc['fid'] = rt.add_features(inc['unit'], [inc])[0]
        count = Counter()
        for nodes, features in rt.results():
            children = sorted(nodes[chname],
                              key=lambda c: features[c].get(sortid, 0))
            line = []
            for dct in self.include:
                line.append(str(features[nodes[dct['unit']]].get(dct['fid'])))
            for ch in children:
                line.append(self.display_unit(features[ch]))
            count['\t'.join(line)] += 1
        cols = ['Count'] + [x['feature'] for x in self.include] + ['Items']
        print('\t'.join(cols))
        for pattern, count in count.most_common():
            print(f'{count}\t{pattern}')
