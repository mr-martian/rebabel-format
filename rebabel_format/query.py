#!/usr/bin/env python3

from rebabel_format.db import RBBLFile, WhereClause
from rebabel_format import utils

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

@dataclass
class Unit:
    query: 'Query'
    type: str
    index: int
    name: Any
    parent: Optional['Unit'] = None
    children: Sequence['Unit'] = field(default_factory=list)

    def __getitem__(self, key):
        return Condition(self.index, key, 'feature')

    def has(self, key):
        self.query.existence_conditionals.append((self.index, key, True))

    def hasnot(self, key):
        self.query.existence_conditionals.append((self.index, key, False))

@dataclass
class Condition:
    left: Any
    right: Any
    operator: str

    def is_compare_ref_feature(self):
        return (self.operator in ['=', '!='] and
                isinstance(self.left, Condition) and
                self.left.operator == 'feature' and
                isinstance(self.right, Unit))

    def toSQL(self, feature_index: dict):
        if self.operator == 'feature':
            idx, _, is_str = feature_index[(self.left, self.right)]
            return f'F{idx}.value', [], is_str
        elif self.is_compare_ref_feature():
            ql, al, sl = self.left.toSQL(feature_index)
            return f'{ql} = U{self.right.index}', ids, False
        def _toSQL(obj):
            nonlocal self, feature_index
            if isinstance(obj, Condition):
                return obj.toSQL(feature_index)
            else:
                return '?', [obj], isinstance(obj, str)
        ql, al, sl = _toSQL(self.left)
        if self.right is None:
            return f'{self.operator}({ql})', al, False
        qr, ar, sr = _toSQL(self.right)
        op = self.operator
        if self.operator == '+' and (sl or sr):
            op = '||'
        elif self.operator in ['startswith', 'contains', 'endswith']:
            # TODO: maybe use create_function to define these better
            op = 'LIKE'
            if self.operator != 'endswith':
                qr = "'%' || " + qr
            if self.operator != 'startswith':
                qr += " || '%'"
        return f'({ql}) {op} ({qr})', al+ar, False

    def features(self):
        if self.operator == 'feature':
            return {(self.left, self.right)}
        ret = set()
        if isinstance(self.left, Condition):
            ret |= self.left.features()
        elif isinstance(self.left, Unit):
            ret |= {(self.left.index, None)}
        if isinstance(self.right, Condition):
            ret |= self.right.features()
        elif isinstance(self.right, Unit):
            ret |= {(self.right.index, None)}
        return ret

    def __add__(self, other):
        return Condition(self, other, '+')
    def __radd__(self, other):
        return Condition(other, self, '+')

    def __sub__(self, other):
        return Condition(self, other, '-')
    def __rsub__(self, other):
        return Condition(other, self, '-')

    def __mul__(self, other):
        return Condition(self, other, '*')
    def __rmul__(self, other):
        return Condition(other, self, '*')

    def __truediv__(self, other):
        return Condition(self, other, '/')
    def __rtruediv__(self, other):
        return Condition(other, self, '/')

    def __mod__(self, other):
        return Condition(self, other, '%')
    def __rmod__(self, other):
        return Condition(other, self, '%')

    def __and__(self, other):
        return Condition(self, other, 'AND')
    def __rand__(self, other):
        return Condition(other, self, 'AND')

    def __or__(self, other):
        return Condition(self, other, 'OR')
    def __ror__(self, other):
        return Condition(other, self, 'OR')

    def __neg__(self):
        return Condition(self, None, '-')

    def __pos__(self):
        return Condition(self, None, '+')

    def __invert__(self):
        return Condition(self, None, 'NOT')

    def __contains__(self, other):
        return Condition(self, other, 'contains')
    def contains(self, other):
        return Condition(self, other, 'contains')

    def startswith(self, other):
        return Condition(self, other, 'startswith')

    def endswith(self, other):
        return Condition(self, other, 'endswith')

    def __lt__(self, other):
        return Condition(self, other, '<')
    def __gt__(self, other):
        return Condition(self, other, '>')
    def __lte__(self, other):
        return Condition(self, other, '<=')
    def __gte__(self, other):
        return Condition(self, other, '>=')
    def __eq__(self, other):
        return Condition(self, other, '=')
    def __ne__(self, other):
        return Condition(self, other, '!=')

class Query:
    def __init__(self, db, type_map=None, feat_map=None):
        self.db = db
        self.type_map = type_map or {}
        self.feat_map = feat_map or {}

        self.units = []
        self.conditionals = {}
        self.features = {} # (uidx, name) => (fidx, ids, is_str)
        self.existence_condtionals = []

    def unit(self, utype, name):
        ret = Unit(self, utype, len(self.units), name)
        self.units.append(ret)
        self.add(ret['meta:active'] == True)
        return ret

    def add(self, condition):
        if not isinstance(condition, Condition):
            raise ValueError(f'Constraint must be a Condition, not {condition.__type__.__name__}.')
        feats = condition.features()
        feat_index = set()
        for fkey in feats:
            idx, oldname = fkey
            if oldname is None:
                continue
            if fkey not in self.features:
                n = len(self.features)
                oldtype = self.units[idx].type
                newtype = utils.map_type(self.type_map, oldtype)
                newname = utils.map_feature(self.feat_map, oldtype, oldname)
                ids = self.db.get_feature_multi_type(newtype, newname, error=False)
                if not ids:
                    remap = ''
                    if oldtype != newtype or oldname != newname:
                        remap = f" (mapping to database type '{newtype}' and feature '{newname}')"
                    raise ValueError(f"No feature '{oldname}' for unit type '{oldtype}{remap}.'")
                is_str = any(x[1] == 'str' for x in ids)
                self.features[fkey] = (n, [x[0] for x in ids], is_str)
            feat_index.add(fkey)
        ckey = tuple(sorted(x[0] for x in feats))
        if ckey not in self.conditionals:
            self.conditionals[ckey] = (condition, feat_index)
        else:
            oldcond, oldfeats = self.conditionals[ckey]
            self.conditionals[ckey] = (oldcond & condition, oldfeats | feat_index)

    def search(self):
        unit_ids = []
        for i in range(len(self.units)):
            cond, feats = self.conditionals[(i,)]
            where, params, _ = cond.toSQL(self.features)
            select = None
            tables = []
            for fkey in feats:
                n, ids, _ = self.features[fkey]
                params += ids
                tables.append(f'features F{n}')
                qs = ','.join(['?']*len(ids))
                where += f' AND F{n}.feature IN ({qs})'
                if not select:
                    select = f'F{n}.unit U{i}'
                else:
                    where += f' AND U{i} = F{n}.unit'
            query = f'SELECT {select} FROM {", ".join(tables)} WHERE {where}'
            # TODO: self.existence_conditionals
            self.db.cur.execute(query, params)
            unit_ids.append(set(x[0] for x in self.db.cur.fetchall()))
            if not unit_ids[-1]:
                return

        intersect = IntersectionTracker(dict(enumerate(unit_ids)))

        def restrict_edge(parent, child, primary):
            nonlocal self, intersect, unit_ids
            self.db.execute_clauses(
                'SELECT parent, child FROM relations',
                WhereClause('parent', list(unit_ids[parent])),
                WhereClause('child', list(unit_ids[child])),
                WhereClause('active', True),
                WhereClause('isprimary', True))
            pairs = self.db.cur.fetchall()
            if not pairs:
                return False
            intersect.restrict(parent, child, pairs)
            return True

        for u in self.units:
            if u.parent is not None:
                if not restrict_edge(u.parent.index, u.index, True):
                    return
            for c in u.children:
                if not restrict_edge(u.index, c.index, False):
                    return

        for ckey in self.conditionals:
            if len(ckey) < 2:
                continue
            cond, feats = self.conditionals[ckey]
            where, params, _ = cond.toSQL(self.features)
            select = [None] * len(ckey)
            tables = []
            for fkey in feats:
                n, ids, _ = self.features[fkey]
                params += ids
                tables.append(f'features F{n}')
                qs = ','.join(['?']*len(ids))
                where += f' AND F{n}.feature IN ({qs})'
                i = fkey[0]
                if not select[i]:
                    select[i] = f'F{n}.unit AS U{i}'
                else:
                    where += f' AND F{n}.unit = U{i}'
            query = f'SELECT {", ".join(select)} FROM {", ".join(tables)} WHERE {where}'
            self.db.cur.execute(query, params)
            sets = self.db.cur.fetchall()
            if not sets:
                return
            for i in range(len(ckey)):
                for j in range(i+1, len(ckey)):
                    intersect.restrict(ckey[i], ckey[j], [(s[i], s[j]) for s in sets])

        intersect.make_dict()

        names = [u.name for u in self.units]
        def combine(cur):
            nonlocal names, intersect
            if len(cur) == len(names):
                yield dict(zip(names, cur))
            else:
                for i in sorted(intersect.possible(dict(enumerate(cur)), len(cur))):
                    yield from combine(cur + [i])
        yield from combine([])

class FeatureQuery:
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
        elif self.operator in ['value_exists', 'value_notexists']:
            pass
        elif '_' in self.operator:
            op = self.operator.split('_')[1]
            neg = False
            if op.startswith('not'):
                neg = True
                op = op[3:]
            where.append(WhereClause('value', self.value,
                                     operator=op, negated=neg))
        db.execute_clauses('SELECT unit, value FROM features', *where)
    def check(self, value):
        if self.operator == 'value_startswith':
            return value.startswith(self.value)
        return True
    def is_notexist(self):
        return ((self.operator == 'value_notexist' and self.value) or
                (self.operator == 'value_exist' and not self.value))
    def get_units(self, db, units=None):
        self.run_query(db, units)
        if self.is_notexist():
            drop = set(x[0] for x in db.cur.fetchall())
            return [u for u in units if u not in drop]
        else:
            return [x[0] for x in db.cur.fetchall() if self.check(x[1])]

class UnitQuery:
    def __init__(self, db, unittype):
        self.db = db
        self.unittype = unittype
        self.features = []
    def add_feature(self, spec):
        if 'feature' not in spec:
            raise ValueError(f'Invalid feature specifier {spec}.')
        feats = self.db.get_feature_multi_type(self.unittype, spec['feature'],
                                               error=True)
        ftypes = sorted(set(f[1] for f in feats))
        if len(ftypes) > 1:
            raise ValueError(f"Feature '{spec['feature']}' has multiple types; found {ftypes}.")
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
            if f.is_notexist():
                continue
            units = f.get_units(self.db, units)
            if len(units) == 0:
                return []
        where = [WhereClause('type', self.unittype)]
        if units:
            where.append(WhereClause('id', units))
        self.db.execute_clauses('SELECT id FROM units', *where)
        units = [x[0] for x in self.db.cur.fetchall()]
        for f in self.features:
            if f.is_notexist():
                units = f.get_units(self.db, units)
            if not units:
                break
        return units

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
        for i, ((on1, on2), opairs) in enumerate(self.pairs):
            if on1 == n1 and on2 == n2:
                self.pairs[i] = ((n1, n2), list(set(opairs) & set(pairs)))
                self.units[n1] = set(p[0] for p in self.pairs[i][1])
                self.units[n2] = set(p[1] for p in self.pairs[i][1])
                break
            elif on1 == n2 and on2 == n1:
                npairs = list(set(opairs) & set((p[1], p[0]) for p in pairs))
                self.pairs[i] = ((n2, n1), npairs)
                self.units[n1] = set(p[1] for p in self.pairs[i][1])
                self.units[n2] = set(p[0] for p in self.pairs[i][1])
                break
        else:
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
            feats = db.get_feature_multi_type(pattern['type'], pattern['order'],
                                              error=False)
            if not feats:
                raise ValueError(f"Cannot find feature '{pattern['order']}' for ordering unit '{name}'.")
            if len(set(f[1] for f in feats)) > 1:
                raise ValueError(f"Cannot sort unit '{name}' by feature '{pattern['order']}' because it has multiple types.")
            order_by[name] = db.get_feature_values(units[name],
                                                   [f[0] for f in feats])
        if 'parent' in pattern and pattern['parent'] is not None:
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
        for typ in oldtypes + [None]:
            if (oldfeat, typ) in feat_map:
                return feat_map[(oldfeat, typ)][0]
        return oldfeat

    if not type_map and not feat_map:
        return
    for k, v in query.items():
        if not isinstance(v, dict):
            continue
        oldtypes = utils.as_list(v['type'])
        v['type'] = [type_map.get(t, t) for t in oldtypes]
        if 'order' in v:
            v['order'] = map_feat(v['order'], oldtypes)
        if 'features' in v:
            for dct in v['features']:
                dct['feature'] = map_feat(dct['feature'], oldtypes)

class ResultTable:
    def __init__(self, db, query, order=None, type_map=None, feat_map=None):
        self.db = db
        self.type_map = type_map or {}
        self.rev_type_map = {v:k for k, v in self.type_map.items()}
        self.feat_map = feat_map or {}
        self.rev_feat_map = {v:k for k, v in self.feat_map.items()}
        map_query(query, self.rev_type_map, self.rev_feat_map)
        self.nodes = list(search(db, query, order=order))
        self.features = []
        self.feature_names = {}
        for result in self.nodes:
            dct = {}
            for l in result.values():
                for uid in utils.as_list(l):
                    dct[uid] = {}
            self.features.append(dct)
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

    def add_features(self, node: str, features: list, map_features=True,
                     error=True) -> None:
        if node is None:
            return
        types = self.types[node]
        feats = []
        feat_types = {}
        for f in features:
            if isinstance(f, int):
                feats.append(f)
                t = self.db.first_clauses('SELECT valuetype FROM tiers',
                                          WhereClause('id', f))
                if t is None:
                    if error:
                        raise ValueError(f'No feature with id {f}.')
                else:
                    feat_types[f] = t[0]
            else:
                if map_features:
                    for (fi, ti), (fo, to) in self.feat_map.items():
                        if fo != f:
                            continue
                        # types will have already been remapped
                        if ti not in utils.as_list(self.types[node]) + [None]:
                            continue
                        f = fi
                        break
                self.db.execute_clauses('SELECT id, valuetype FROM tiers',
                                        WhereClause('name', f),
                                        WhereClause('unittype', types))
                found_any = not error
                for i, t in self.db.cur.fetchall():
                    found_any = True
                    feats.append(i)
                    feat_types[i] = t
                    self.feature_names[i] = f
                if not found_any:
                    raise ValueError(f'Feature {f} does not exist for unit type {types}.')
        units = list(set(self._node_ids(node)))
        self.db.execute_clauses('SELECT unit, feature, value FROM features',
                                WhereClause('unit', units),
                                WhereClause('feature', feats))
        for u, f, v in self.db.cur.fetchall():
            v = self.db.interpret_value(v, feat_types[f])
            for rid in self.unit2results[u]:
                if u in utils.as_list(self.nodes[rid][node]):
                    self.features[rid][u][self.feature_names[f]] = v

    def add_tier(self, node: str, tier: str) -> None:
        feats = []
        skip = set()
        for (fi, ti), (fo, to) in self.feat_map.items():
            if ti and ti not in self.types[node]:
                continue
            if fo.startswith(tier+':'):
                f = self.db.first_clauses('SELECT id FROM tiers',
                                          WhereClause('name', fi),
                                          WhereClause('unittype', self.types[node]))
                if f:
                    feats.append(f[0])
                    self.feature_names[f[0]] = fo
            if fi.startswith(tier+':'):
                skip.add(fi)
        self.db.execute_clauses('SELECT id, name FROM tiers',
                                WhereClause('unittype', self.types[node]),
                                WhereClause('name', tier+':', operator='startswith'))
        for i, f in self.db.cur.fetchall():
            if f in skip:
                continue
            feats.append(i)
            self.feature_names[i] = f
        self.add_features(node, feats, map_features=False)

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
