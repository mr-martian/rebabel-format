#!/usr/bin/env python3

from .db import RBBLFile
from .config import parse_feature

from collections import defaultdict
import re

class FeatureQuery:
    like_escape = re.compile('([%_$])')
    def __init__(self, featid, value=None, operator=None):
        self.featid = featid
        self.value = value
        self.operator = operator
    def query(self, unitids=None):
        ret = f'SELECT f.unit, f.value FROM features f WHERE f.feature = ?'
        params = [self.featid]
        if unitids is not None:
            if isinstance(unitids, list) and len(unitids) > 0:
                q = ', '.join(['?']*len(unitids))
                ret += f' AND f.unit IN ({q})'
                params += unitids
            elif isinstance(unitids, str):
                ret += f' AND f.unit = {unitids}'
        if self.operator is None:
            pass
        elif self.operator == 'value':
            ret += ' AND f.value = ?'
            params.append(self.value)
        elif self.operator == 'value_startswith':
            ret += ' AND f.value LIKE ?'
            v = self.value
            if '%' in v or '_' in v:
                v = self.like_escape.subn(r'$\1', v)[0]
                ret += " ESCAPE '$'"
            v += '%'
            params.append(v)
        return ret, params
    def check(self, value):
        if self.operator == 'value_startswith':
            return value.startswith(self.value)
        return True
    def get_units(self, db, units=None):
        qr, params = self.query(units)
        db.cur.execute(qr, params)
        return [x[0] for x in db.cur.fetchall() if self.check(x[1])]

class UnitQuery:
    def __init__(self, db, unittype):
        self.db = db
        self.unittype = unittype
        self.features = []
    def add_feature(self, spec):
        if 'tier' not in spec or 'feature' not in spec:
            raise ValueError(f'Invalid feature specifier {spec}.')
        fid, ftyp = self.db.get_feature(self.unittype, spec['tier'],
                                        spec['feature'], error=True)
        if ftyp == 'ref':
            raise NotImplementedError(f'Reference features are not yet supported ({spec}).')
        v = [k for k in spec if k.startswith('value')]
        if len(v) > 1:
            raise ValueError(f'Cannot specify multiple value constrains on a single feature, found {v}.')
        if v:
            fq = FeatureQuery(fid, spec[v[0]], v[0])
        else:
            fq = FeatureQuery(fid)
        self.features.append(fq)
    def check(self, fq, ids):
        if not ids:
            return []
        q, p = fq.query(ids)
        self.db.cur.execute(q, p)
        ret = []
        for i, v in self.db.cur.fetchall():
            if fq.check(v):
                ret.append(i)
        return ret
    def get_units(self):
        units = None
        for f in self.features:
            units = f.get_units(self.db, units)
            if units == []:
                return []
        qr = 'SELECT u.id FROM units u WHERE type'
        params = []
        if isinstance(self.unittype, str):
            qr += ' = ?'
            params.append(self.unittype)
        elif isinstance(self.unittype, list):
            qr += ' IN (' + ', '.join(['?']*len(self.unittype)) + ')'
            params += self.unittype
        else:
            raise ValueError(f"Invalid unit type specifier '{self.unittype}'.")
        if units:
            plc = ', '.join(['?']*len(units))
            qr += f' AND id IN ({plc})'
            params += units
        self.db.cur.execute(qr, params)
        ids = [x[0] for x in self.db.cur.fetchall()]
        return ids

class IntersectionTracker:
    def __init__(self, units):
        self.units = units
        self.pairs = []
        self.restrictions = {} # A.name => A.id => B.name => B.id
    def lookup(self, A, B, uid):
        if A not in self.restrictions:
            return self.units[B]
        if uid not in self.restrictions[A]:
            return set()
        if B not in self.restrictions[A][uid]:
            return self.units[B]
        return self.restrictions[A][uid][B].copy()
    def restrict(self, n1, n2, pairs):
        self.pairs.append(((n1, n2), pairs))
        todo = [n1, n2]
        while todo:
            n = todo.pop()
            s = self.units[n].copy()
            for (n1, n2), pls in self.pairs:
                if n1 == n or n2 == n:
                    if not pls:
                        s.clear()
                        break
                    s1, s2 = map(set, zip(*pls))
                    if n1 == n:
                        s = s.intersection(s1)
                    elif n2 == n:
                        s = s.intersection(s2)
            if len(s) == len(self.units[n]):
                continue
            else:
                self.units[n] = s
            for i, ((n1, n2), pls) in enumerate(self.pairs):
                if n1 == n:
                    npls = [p for p in pls if p[0] in s]
                    if len(npls) < len(pls):
                        todo.append(n2)
                        self.pairs[i] = ((n1, n2), npls)
                elif n2 == n:
                    npls = [p for p in pls if p[1] in s]
                    if len(npls) < len(pls):
                        todo.append(n1)
                        self.pairs[i] = ((n1, n2), npls)
    def make_dict(self):
        for (n1, n2), pairs in self.pairs:
            if n1 not in self.restrictions:
                self.restrictions[n1] = {}
            if n2 not in self.restrictions:
                self.restrictions[n2] = {}
            for a, b in pairs:
                if a not in self.restrictions[n1]:
                    self.restrictions[n1][a] = {}
                if n2 not in self.restrictions[n1][a]:
                    self.restrictions[n1][a][n2] = set()
                self.restrictions[n1][a][n2].add(b)
                if b not in self.restrictions[n2]:
                    self.restrictions[n2][b] = {}
                if n1 not in self.restrictions[n2][b]:
                    self.restrictions[n2][b][n1] = set()
                self.restrictions[n2][b][n1].add(a)
    def possible(self, dct, name):
        ret = self.units[name].copy()
        for k in dct:
            ret = ret.intersection(self.lookup(k, name, dct[k]))
        return ret

def search(db, query):
    units = {}
    rels = [] # [(parent, child), ...]
    seq_rels = [] # [(A, B, type, immediate), ...]
    for name, pattern in query.items():
        if not isinstance(pattern, dict):
            continue
        if 'type' not in pattern:
            raise ValueError(f'Missing unittype for {name}.')
        uq = UnitQuery(db, pattern['type'])
        for spec in pattern.get('features', []):
            uq.add_feature(spec)
        units[name] = uq.get_units()
        if not units[name]:
            return
        if 'parent' in pattern:
            if pattern['parent'] not in query:
                raise ValueError(f'No node named {pattern["parent"]} (referenced by {name}).')
            rels.append((pattern['parent'], name))
        if 'next' in pattern:
            if pattern['next'] not in query:
                raise ValueError(f'No node named {pattern["next"]} (referenced by {name}).')
            if pattern['type'] != query[pattern['next']].get('type'):
                raise ValueError(f'Adjacency constraints only make sense for units of the same type ({name} is {pattern["type"]} but {pattern["next"]} is {query[pattern["next"]].get("type")})')
            seq_rels.append((name, pattern['next'], pattern['type'], True))
    # TODO: I expect there's some more intelligent way to do this part of the
    # querying if we pre-compute the right order to search for certain things
    # and then work them into the unit queries. Unfortunately, by ensuring
    # that the code below will return things in a consistent order, I've
    # probably doomed us to forever maintain that order per Hyrum's Law.
    # DGS 2023-11-21
    intersect = IntersectionTracker({x: set(y) for x, y in units.items()})
    for parent, child in rels:
        q1 = ', '.join(['?']*len(units[parent]))
        q2 = ', '.join(['?']*len(units[child]))
        db.cur.execute(
            f'SELECT parent, child FROM relations WHERE parent IN ({q1}) AND child IN ({q2}) AND active = ?',
            units[parent] + units[child] + [True],
        )
        pairs = set(db.cur.fetchall())
        if not pairs:
            return
        intersect.restrict(parent, child, pairs)
    for A, B, typ, immediate in seq_rels:
        q1 = ', '.join(['?']*len(units[A]))
        q2 = ', '.join(['?']*len(units[B]))
        fid, ftyp = db.get_feature(typ, 'meta', 'index', error=True)
        db.cur.execute(
            f'SELECT A.unit, B.unit FROM features A, features B WHERE A.value + 1 = B.value AND A.feature = ? AND B.feature = ? AND A.unit IN ({q1}) AND B.unit IN ({q2})',
            [fid, fid] + units[A] + units[B],
        )
        pairs = set(db.cur.fetchall())
        if not pairs:
            return
        intersect.restrict(A, B, pairs)
    intersect.make_dict()
    seq = sorted(units.keys())
    def combine(cur):
        nonlocal seq, units, intersect
        if len(cur) == len(seq):
            yield dict(zip(seq, cur))
        else:
            u = intersect.possible(dict(zip(seq, cur)), seq[len(cur)])
            for i in sorted(u):
                yield from combine(cur + [i])
    yield from combine([])
