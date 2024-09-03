#!/usr/bin/env python3

from rebabel_format.db import RBBLFile, WhereClause
from rebabel_format.config import parse_feature
from rebabel_format import utils

from collections import defaultdict
import re

class FeatureQuery:
    like_escape = re.compile('([%_$])')
    def __init__(self, featid, value=None, operator=None):
        self.featid = featid
        self.value = value
        self.operator = operator
    def run_query(self, db, unitids=None):
        where = [WhereClause('feature', self.featid)]
        if unitids is not None:
            where.append(WhereClause('unit', unitids))
        if self.operator is None:
            pass
        elif self.operator == 'value':
            where.append(WhereClause('value', self.value))
        elif self.operator == 'value_startswith':
            v = self.value
            if '%' in v or '_' in v:
                where.append(WhereClause('value',
                                         self.like_escape.sub(r'$\1', v) + '%',
                                         operator='LIKE',
                                         suffix="ESCAPE '$'"))
            else:
                where.append(WhereClause('value', v + '%', operator='LIKE'))
        db.execute_clauses('SELECT unit, value FROM features', *where)
    def check(self, value):
        if self.operator == 'value_startswith':
            return value.startswith(self.value)
        return True
    def get_units(self, db, units=None):
        self.run_query(db, units)
        return [x[0] for x in db.cur.fetchall() if self.check(x[1])]

class UnitQuery:
    def __init__(self, db, unittype):
        self.db = db
        self.unittype = unittype
        self.features = []
    def add_feature(self, spec):
        if 'tier' not in spec or 'feature' not in spec:
            raise ValueError(f'Invalid feature specifier {spec}.')
        feats = self.db.get_feature_multi_type(self.unittype, spec['tier'],
                                               spec['feature'], error=True)
        ftypes = sorted(set(f[1] for f in feats))
        if len(ftypes) > 1:
            raise ValueError(f"Feature '{spec['tier']}:{spec['feature']}' has multiple types; found {ftypes}.")
        ftyp = ftypes[0]
        fid = [f[0] for f in feats]
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
        fq.run_query(self.db, ids)
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
        where = [WhereClause('type', self.unittype)]
        if units:
            where.append(WhereClause('id', units))
        self.db.execute_clauses('SELECT id FROM units', *where)
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

def make_sequence(units, order, multi_leaf):
    base = [u for u in (order or sorted(units)) if u != multi_leaf]
    ret = [u for u in base if u in units]
    ret += [u for u in sorted(units) if u not in ret and u != multi_leaf]
    if multi_leaf:
        ret.append(multi_leaf)
    return ret

def search(db, query, order=None):
    units = {}
    order_by = {}
    rels = [] # [(parent, child), ...]
    seq_rels = [] # [(A, B, type, immediate), ...]
    multi_leaf = None
    for name, pattern in query.items():
        if not isinstance(pattern, dict):
            continue
        if 'type' not in pattern:
            raise ValueError(f'Missing unittype for {name}.')
        uq = UnitQuery(db, pattern['type'])
        for spec in pattern.get('features', []):
            uq.add_feature(spec)
        units[name] = uq.get_units()
        if pattern.get('multiple', False):
            if multi_leaf is not None:
                raise ValueError(f'Cannot have both {multi_leaf} and {name} as multi-nodes.')
            multi_leaf = name
        elif not units[name]:
            return
        if 'order' in pattern:
            tier, feature = parse_feature(pattern['order'])
            feats = db.get_feature_multi_type(pattern['type'], tier, feature,
                                              error=False)
            if not feats:
                raise ValueError(f"Cannot find feature '{spec['tier']}:{spec['feature']}' for ordering unit '{name}'.")
            if len(set(f[1] for f in feats)) > 1:
                raise ValueError(f"Cannot sort unit '{name}' by feature '{spec['tier']}:{spec['feature']}' because it has multiple types.")
            order_by[name] = db.get_feature_values(units[name],
                                                   [f[0] for f in feats])
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
        if parent == multi_leaf:
            raise ValueError(f'Node {parent} cannot have both children and multiple = true.')
        db.execute_clauses('SELECT parent, child FROM relations',
                           WhereClause('parent', units[parent]),
                           WhereClause('child', units[child]),
                           WhereClause('active', True))
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
    seq = make_sequence(set(units.keys()), order, multi_leaf)
    def sort_units(name, uids):
        nonlocal order_by
        if name not in order_by:
            return sorted(uids)
        else:
            val = []
            noval = []
            for u in uids:
                v = order_by[name].get(u)
                if v is None:
                    noval.append(u)
                else:
                    val.append((v, u))
            return [u for v, u in sorted(val)] + sorted(noval)
    def combine(cur):
        nonlocal seq, units, intersect, multi_leaf
        if len(cur) == len(seq):
            yield dict(zip(seq, cur))
        else:
            name = seq[len(cur)]
            u = sort_units(name,
                           intersect.possible(dict(zip(seq, cur)), name))
            if name == multi_leaf:
                yield from combine(cur + [u])
            else:
                for i in u:
                    yield from combine(cur + [i])
    yield from combine([])

def map_query(query, type_map, feat_map):
    '''Mutate `query` based on mappings'''

    def map_feat(oldfeat, oldtypes):
        nonlocal feat_map
        tier, feat = parse_feature(oldfeat)
        for typ in oldtypes + [None]:
            if ((tier, feat), typ) in feat_map:
                return feat_map[((tier, feat), typ)][0]
        return tier, feat

    if not type_map and not feat_map:
        return
    for k, v in query.items():
        if not isinstance(v, dict):
            continue
        oldtypes = utils.as_list(v['type'])
        v['type'] = [type_map.get(t, t) for t in oldtypes]
        if 'order' in v:
            t, f = map_feat(v['order'], oldtypes)
            v['order'] = {'tier': t, 'feature': f}
        if 'features' in v:
            for dct in v['features']:
                dct['tier'], dct['feature'] = map_feat(dct, oldtypes)

class ResultTable:
    def __init__(self, db, query, order=None, type_map=None, feat_map=None):
        self.db = db
        self.type_map = type_map or {}
        self.rev_type_map = {v:k for k, v in self.type_map.items()}
        self.feat_map = feat_map or {}
        self.rev_feat_map = {v:k for k, v in self.feat_map.items()}
        map_query(query, self.rev_type_map, self.rev_feat_map)
        self.nodes = list(search(db, query, order=order))
        self.features = [{v: {} for v in r.values()} for r in self.nodes]
        self.types = {k: v['type'] for k, v in query.items()
                      if isinstance(v, dict)}
        self.unit2results = defaultdict(list)
        for i, result in enumerate(self.nodes):
            for uid in result.values():
                for u in utils.as_list(uid):
                    self.unit2results[u].append(i)

    def _node_ids(self, name: str):
        for result in self.nodes:
            ret = result[name]
            if isinstance(ret, int):
                yield ret
            else:
                yield from ret

    def get_type(self, uid) -> str:
        db_type = self.db.get_unit_type(uid)
        return self.type_map.get(db_type, db_type)

    def get_relations(self, parents, children):
        self.db.execute_clauses('SELECT parent, child FROM relations',
                                WhereClause('parent', parents),
                                WhereClause('children', children))
        return self.db.cur.fetchall()

    def add_features(self, node: str, features: list, map_features=True):
        if node is None:
            return [None] * len(features)
        types = self.types[node]
        feats = []
        feat_types = {}
        for f in features:
            if isinstance(f, int):
                feats.append(f)
                t = self.db.first_clauses('SELECT valuetype FROM tiers',
                                          WhereClause('id', f))
                if t is None:
                    raise ValueError(f'No feature with id {f}.')
                feat_types[f] = t[0]
            else:
                tier, feature = parse_feature(f)
                if map_features:
                    for typ in self.types[node] + [None]:
                        key = ((tier, feature), typ)
                        if key in self.rev_feature_map:
                            tier, feature = self.rev_feature_map[key][0]
                # TODO: If there's multiple types, this should get all
                # features. We probably want to return a dict from this
                # function.
                f = self.db.first_clauses('SELECT id, valuetype FROM tiers',
                                          WhereClause('tier', tier),
                                          WhereClause('feature', feature),
                                          WhereClause('unittype', types))
                if f is None:
                    raise ValueError(f'Feature {tier}:{feature} does not exist for unit type {types}.')
                feats.append(f[0])
                feat_types[f[0]] = f[1]
        units = list(set(self._node_ids(node)))
        self.db.execute_clauses('SELECT unit, feature, value FROM features',
                                WhereClause('unit', units),
                                WhereClause('feature', feats))
        for u, f, v in self.db.cur.fetchall():
            v = self.db.interpret_value(v, feat_types[f])
            for rid in self.unit2results[u]:
                if u in utils.as_list(self.nodes[rid][node]):
                    self.features[rid][u][f] = v
        return feats

    def add_tier(self, node: str, tier: str, prefix=False):
        tier_list = [tier]
        if prefix:
            self.db.execute_clauses('SELECT DISTINCT tier FROM tiers',
                                    WhereClause('unittype', self.types[node]))
            tier_list = [t[0] for t in self.db.cur.fetchall()
                         if t[0].startswith(tier)]
        names = []
        feats = []
        skip = set()
        for (fi, ti), (fo, to) in self.feat_map.items():
            if ti and ti not in self.types[node]:
                continue
            if fo[0] == tier or (prefix and fo[0].startswith(tier)):
                f = self.db.first_clauses('SELECT id FROM tiers',
                                          WhereClause('tier', fi[0]),
                                          WhereClause('feature', fi[1]),
                                          WhereClause('unittype', self.types[node]))
                if f:
                    feats.append(f[0])
                    names.append(fo)
            if fi[0] == tier or (prefix and fi[0].startswith(tier)):
                skip.add(fi)
        self.db.execute_clauses('SELECT id, tier, feature FROM tiers',
                                WhereClause('unittype', self.types[node]),
                                WhereClause('tier', tier_list))
        for i, t, f in self.db.cur.fetchall():
            if (t, f) in skip:
                continue
            feats.append(i)
            names.append((t, f))
        ids = self.add_features(node, feats, map_features=False)
        return dict(zip(names, ids))

    def add_children(self, node, child_type):
        if not self.nodes:
            return
        units = list(set(self._node_ids(node)))
        children = self.db.get_children(units, child_type)
        name = node + '_children'
        while name in self.nodes[0]:
            name += '*'
        for i, result in enumerate(self.nodes):
            result[name] = []
            ls = result[node]
            if isinstance(ls, int):
                ls = [ls]
            for p in ls:
                result[name] += children[p]
            for c in result[name]:
                self.features[i].setdefault(c, {})
                self.unit2results[c].append(i)
        self.types[name] = child_type
        return name

    def results(self):
        yield from zip(self.nodes, self.features)
