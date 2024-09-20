#!/usr/bin/env python3

from rebabel_format.process import SearchProcess
from rebabel_format.parameters import Parameter

from functools import lru_cache

class Concordance(SearchProcess):
    '''Perform a concordance search'''

    name = 'concordance'
    center = Parameter(default='Center',
                       help='name of unit in query to center concordance window around')
    width = Parameter(default=2, help='width of concordance window')
    label = Parameter(type=list,
                      help='features of units from the query to label each output line with')
    print = Parameter(type=str, help='feature to display for units in the concordance window')

    @lru_cache(maxsize=100)
    def get_children(db, uid, child_type):
        return db.get_units(child_type, parent=uid)

    def get_child_bound(self, uid, child_type, bound, right):
        children = Concordance.get_children(self.db, uid, child_type)
        fid, ftype = self.db.get_feature(child_type, 'meta:index')
        idx = self.db.get_feature_values(children, fid)
        children.sort(key=lambda c: idx.get(c, bound))
        if right:
            for u in children:
                if idx.get(u, bound) > bound:
                    return u
        else:
            for u in reversed(children):
                if idx.get(u, bound) < bound:
                    return u

    def get_edge(self, uid, child_type, right=True):
        if uid is None:
            return None
        import math
        bound = -math.inf if right else math.inf
        u = self.get_child_bound(uid, child_type, bound=bound, right=right)
        if u is None:
            u = self.get_edge(self.get_next(uid, right), child_type, right)
        return u

    def get_next(self, uid, right=True):
        if uid is None:
            return None
        utype = self.db.get_unit_type(uid)
        fid, ftype = self.db.get_feature(utype, 'meta:index')
        idx = self.db.get_feature_value(uid, fid)
        parent = self.db.get_parent(uid)
        if parent is None:
            return None
        ret = self.get_child_bound(parent, utype, idx, right)
        if ret is None:
            ret = self.get_edge(self.get_next(parent, right), utype, right)
        return ret

    def get_span(self, uid, width):
        left = []
        right = []
        l = uid
        r = uid
        for i in range(width):
            l = self.get_next(l, False)
            r = self.get_next(r, True)
            left.append(l)
            right.append(r)
        ret = list(reversed(left)) + [uid] + right
        return [u for u in ret if u is not None]

    def print_span(self, span):
        utype = self.db.get_unit_type(span[0])
        fid, ftype = self.db.get_feature(utype, self.print)
        print(' '.join(str(self.db.get_feature_value(u, fid)) for u in span))

    def per_result(self, result):
        self.print_label(result, self.label)
        self.print_span(self.get_span(result[self.center], self.width))
