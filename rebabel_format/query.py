#!/usr/bin/env python3

from rebabel_format.db import RBBLFile, WhereClause
from rebabel_format import utils

from collections import Counter, defaultdict
from dataclasses import dataclass, field
import re
from typing import Any, Optional, Sequence

@dataclass
class Unit:
    query: 'Query'
    type: str
    index: int
    name: Any
    order: Optional[str] = None

    def __getitem__(self, key):
        return Condition(self.index, key, 'feature')

    def parent(self, other):
        if not isinstance(other, Unit):
            raise ValueError(f'Parent must be a Unit, not {other.__type__.__name__}')
        return Condition(self.index, other.index, 'parent')

    def child(self, other):
        if not isinstance(other, Unit):
            raise ValueError(f'Child must be a Unit, not {other.__type__.__name__}')
        return Condition(self.index, other.index, 'child')

    def subquery(self, min=1, max=None):
        ret = Query(self.query.db, self.query.type_map, self.query.feat_map)
        u = ret.unit(self.type, self.name)
        self.query.subqueries.append((ret, self.index, min, max))
        return ret, u

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

    def toSQL(self, query):
        if self.operator == 'feature':
            idx, _, _, is_str = query.get_feature(self.left, self.right, False)
            return f'F{idx}.value', [], is_str
        elif self.operator == 'exists':
            idx, ids, _, is_str = query.get_feature(self.left, self.right, True)
            qs = ', '.join(['?']*len(ids))
            return f'EXISTS (SELECT NULL FROM features F{idx} WHERE F{idx}.unit = U{self.left} AND F{idx}.feature IN ({qs}))', ids, False
        elif self.is_compare_ref_feature():
            ql, al, sl = self.left.toSQL(query)
            return f'{ql} = U{self.right.index}', al, False
        elif self.operator == 'parent':
            return f'EXISTS (SELECT NULL FROM relations WHERE parent = U{self.right} AND child = U{self.left} AND isprimary = ? AND active = ?)', [True, True], False
        elif self.operator == 'child':
            return f'EXISTS (SELECT NULL FROM relations WHERE child = U{self.right} AND parent = U{self.left} AND isprimary = ? AND active = ?)', [False, True], False
        def _toSQL(obj):
            nonlocal self, query
            if isinstance(obj, Condition):
                return obj.toSQL(query)
            else:
                return '?', [obj], isinstance(obj, str)
        qr, ar, sr = _toSQL(self.right)
        if self.left is None:
            return f'{self.operator}({qr})', ar, False
        ql, al, sl = _toSQL(self.left)
        op = self.operator
        if self.operator == '+' and (sl or sr):
            op = '||'
        elif self.operator in ['startswith', 'contains', 'endswith']:
            # TODO: maybe use create_function to define these better
            op = 'LIKE'
            if self.operator != 'startswith':
                qr = "'%' || " + qr
            if self.operator != 'endsswith':
                qr += " || '%'"
        return f'({ql}) {op} ({qr})', al+ar, False

    def add_to_query(self, query):
        if self.operator in ['parent', 'child']:
            table = f'R{query.relation_count}'
            query.relation_count += 1
            query.select_tables.append(f'relations {table}')
            query.where_conds += [
                f'{table}.isprimary = ?',
                f'{table}.active = ?',
            ]
            query.params += [(self.operator == 'parent'), True]
            if self.operator == 'parent':
                query.where_conds += [
                    f'{table}.parent = U{self.right}',
                    f'{table}.child = U{self.left}',
                ]
            else:
                query.where_conds += [
                    f'{table}.parent = U{self.left}',
                    f'{table}.child = U{self.right}',
                ]
        else:
            s, p, _ = self.toSQL(query)
            query.where_conds.append(s)
            query.params += p

    def flatten(self):
        if self.operator == 'AND':
            yield from self.left.flatten()
            yield from self.right.flatten()
        else:
            yield self

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
        return Condition(None, self, '-')

    def __pos__(self):
        return Condition(None, self, '+')

    def __invert__(self):
        return Condition(None, self, 'NOT')

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

    def exists(self):
        if self.operator != 'feature':
            raise ValueError('.exists() only makes sense for features.')
        return Condition(self.left, self.right, 'exists')

class Query:
    token_re = re.compile(r'[()\.]|"(?:[^"\\]|\\.)*"|[^\s()\."]+')
    def __init__(self, db, type_map=None, feat_map=None):
        self.db = db
        self.type_map = type_map or {}
        self.feat_map = feat_map or {}

        self.units = []
        self.name2unit = {}
        self.conditional = None
        self.features = {} # (uidx, name) => (fidx, ids, is_str)
        self.subqueries = [] # [(query, uidx, min, max), ...]
        self.order = defaultdict(dict)

        self.select_cols = []
        self.select_tables = []
        self.where_conds = []
        self.params = []
        self.relation_count = 0

        self.results = []
        self.unit_ids = []

    def add_clause(self, clause):
        txt, params = clause.toSQL()
        self.where_conds.append(txt)
        self.params += params

    def unit(self, utype, name):
        ret = Unit(self, utype, len(self.units), name)
        self.units.append(ret)
        self.add(ret['meta:active'] == True)
        self.name2unit[name] = ret
        self.select_cols.append(f'TU{ret.index}.id AS U{ret.index}')
        self.select_tables.append(f'units TU{ret.index}')
        self.add_clause(WhereClause(f'TU{ret.index}.type',
                                    utils.map_type(self.type_map, utype)))
        return ret

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.units[key]
        else:
            return self.name2unit[key]

    def add_feature(self, idx, oldname, for_exist):
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
        n = len(self.features)
        if not for_exist:
            self.select_tables.append(f'features F{n}')
            self.where_conds.append(f'F{n}.unit = U{idx}')
            qs = ','.join(['?']*len(ids))
            self.where_conds.append(f'F{n}.feature IN ({qs})')
            self.params += [x[0] for x in ids]
        return len(self.features), [x[0] for x in ids], set(x[1] for x in ids), is_str

    def get_feature(self, idx, name, for_exist):
        if (idx, name, for_exist) not in self.features:
            self.features[(idx, name, for_exist)] = self.add_feature(idx, name, for_exist)
        return self.features[(idx, name, for_exist)]

    def add(self, condition):
        if not isinstance(condition, Condition):
            raise ValueError(f'Constraint must be a Condition, not {condition.__type__.__name__}.')
        if self.conditional is None:
            self.conditional = condition
        else:
            self.conditional = self.conditional & condition

    def prepare_search(self, parent_ids=None):
        if self.results:
            return
        if parent_ids:
            self.add_clause(WhereClause('U0', parent_ids))
        if self.conditional is not None:
            for c in self.conditional.flatten():
                c.add_to_query(self)
        query = f'SELECT {", ".join(self.select_cols)} FROM {", ".join(self.select_tables)} WHERE {" AND ".join(self.where_conds)}'
        self.db.cur.execute(query, self.params)
        self.results = list(set(self.db.cur.fetchall()))

        self.unit_ids = [set() for i in range(len(self.units))]
        for r in self.results:
            for i in range(len(self.units)):
                self.unit_ids[i].add(r[i])

        for i, u in enumerate(self.units):
            if u.order:
                _, ids, types, _ = self.get_feature(i, u.order, False)
                if len(types) > 1:
                    raise ValueError(f"Cannot sort unit '{u.name}' by feature '{u.order}' because it has multiple types after mapping.")
                t = list(types)[0]
                self.db.execute_clauses(
                    'SELECT unit, value FROM features',
                    WhereClause('feature', ids),
                    WhereClause('unit', list(self.unit_ids[i])))
                self.order[i] = {u: (0, self.db.interpret_value(v, t))
                                 for u, v in self.db.cur.fetchall()}

        self.results.sort(
            key=lambda tup: tuple([self.order[i].get(u, (1, u))
                                   for i, u in enumerate(tup)]))

    def get_results(self, parent=None):
        names = [u.name for u in self.units]
        for result in self.results:
            if parent is not None and result[0] != parent:
                continue
            dct = dict(zip(names, result))
            count = Counter()
            ok = True
            for sub, idx, mn, mx in self.subqueries:
                n = count[idx]
                count[idx] += 1
                results = list(sub.get_results(result[idx]))
                if mn is not None and len(results) < mn:
                    ok = False
                    break
                if mx is not None and len(results) > mx:
                    ok = False
                    break
                dct[(names[idx], n)] = results
            if ok:
                yield dct

    def search(self):
        self.prepare_search()
        for sub, idx, mn, mx in self.subqueries:
            sub.prepare_search(list(self.unit_ids[idx]))
            if mn is not None and mn > 0 and not sub.results:
                return
        yield from self.get_results()

    def add_line(self, line, linenumber=0):
        prefix = ''
        if linenumber != 0:
            prefix = f'Error on line {linenumber}: '
        tokens = []
        for tok in Query.token_re.findall(line):
            if tok.startswith('#'):
                break
            tokens.append(tok)
        if not tokens:
            return
        if tokens[0].lower() == 'unit':
            if len(tokens) < 3:
                raise ValueError(prefix+'Missing unit type.')
            self.unit(tokens[2:], tokens[1])
            return
        precedence = {
            '.': 7, 'has': 7, 'exists': 7, 'feature': 7,
            '*': 6, '/': 6, '%': 6,
            '+': 5, '-': 5,
            'contains': 4, 'startswith': 4, 'endswith': 4, 'parent': 4, 'child': 4,
            '<': 3, '>': 3, '<=': 3, '>=': 3, '=': 3, '!=': 3,
            'not': 2, 'NOT': 2,
            'and': 1, 'AND': 1,
            'or': 0, 'OR': 0,
        }
        op_rename = {
            '.': 'feature', 'has': 'exists', 'not': 'NOT', 'and': 'AND', 'or': 'OR',
        }
        old_stacks = []
        stack = []
        for tok in tokens:
            ltok = tok.lower()
            if tok == '(':
                if stack and stack[-1].right is not None:
                    raise ValueError(prefix+'Expected operator, not open parenthesis.')
                old_stacks.append(stack)
                stack = []
            elif tok == ')':
                if not stack:
                    raise ValueError(prefix+'Empty parentheses.')
                if not old_stacks:
                    raise ValueError(prefix+'Close parenthesis without preceding open parenthesis.')
                if stack[-1].right is None:
                    raise ValueError(prefix+'Expected operand before closing parenthesis.')
                val = stack[0]
                stack = old_stacks.pop()
                if stack:
                    stack[-1].right = val
                else:
                    stack.append(val)
            elif ltok == 'not':
                if not stack:
                    stack.append(Condition(None, None, 'NOT'))
                elif isinstance(stack[-1], Condition) and stack[-1].right is None:
                    stack.append(Condition(None, None, 'NOT'))
                    stack[-2].right = stack[-1]
                else:
                    raise ValueError(prefix+'Unexpected NOT.')
            elif ltok in precedence:
                op = op_rename.get(ltok, ltok)
                if op in ['feature', 'parent', 'child']:
                    if not stack:
                        pass
                    elif isinstance(stack[-1], Unit):
                        stack[-1] = stack[-1].index
                    elif isinstance(stack[-1], Condition) and isinstance(stack[-1].right, Unit):
                        stack[-1].right = stack[-1].right.index
                    else:
                        raise ValueError(prefix+f'Operator {ltok} can only apply to unit names.')
                if len(stack) == 1 and not isinstance(stack[0], Condition):
                    stack[0] = Condition(stack[0], None, op)
                    continue
                if not stack or stack[-1].right is None:
                    raise ValueError(f'{prefix}Unexpected operator {tok}.')
                for i in range(len(stack)):
                    if precedence[stack[i].operator] > precedence[ltok]:
                        stack[i] = Condition(stack[i], None, op)
                        stack = stack[:i+1]
                        if i > 0:
                            stack[i-1].right = stack[i]
                        break
                else:
                    stack.append(Condition(stack[-1].right, None, op))
                    stack[-2].right = stack[-1]
            else:
                val = tok
                if val.isdigit() or (val[0] in '+-' and val[1:].isdigit()):
                    val = int(val)
                elif ltok in ['true', 'false']:
                    val = (ltok == 'true')
                elif val.startswith('"'):
                    val = tok[1:-1].replace('\\n', '\n')
                elif stack and isinstance(stack[-1], Condition) and stack[-1].operator in ['feature', 'exists']:
                    pass
                else:
                    for u in self.units:
                        if u.name == val:
                            val = u
                            if stack and isinstance(stack[-1], Condition) and stack[-1].operator in ['parent', 'child']:
                                val = u.index
                            break
                    else:
                        raise ValueError(f'{prefix}Unit "{val}" is not defined.')
                if stack:
                    if stack[-1].right is not None:
                        raise ValueError(f'{prefix}Expected operator, not {tok}.')
                    stack[-1].right = val
                else:
                    stack.append(val)
        if old_stacks:
            raise ValueError(prefix+'Parenthesis opened but not closed.')
        if stack[-1].right is None:
            raise ValueError(prefix+'Missing right operand.')
        if not isinstance(stack[-1].right, Condition) and stack[-1].operator == 'NOT':
            raise ValueError(prefix+'Cannot negate value.')
        self.add(stack[0])

    def parse_query_dict(self, query, order=None):
        if not order and isinstance(query.get('order'), list):
            order = query['order']
        done = set(order or [])
        seq = [k for k in order or [] if k in query]
        seq += [k for k in sorted(query.keys()) if k not in done]
        units = {}
        for key in seq:
            pattern = query[key]
            if not isinstance(pattern, dict):
                continue
            if 'type' not in pattern:
                raise ValueError(f'Missing unit type for {key}.')
            self.unit(pattern['type'], key)
        for key in seq:
            self.parse_unit_dict(query[key], self.name2unit[key])

    def parse_unit_dict(self, pattern, unit):
        op_lookup = {'lt': '<', 'lte': '<=', 'gt': '>', 'gte': '>=', '=': '=',
                     'startswith': 'startswith', 'endswith': 'endswith',
                     'contains': 'contains'}
        for spec in pattern.get('features', []):
            if isinstance(spec, str):
                spec = spec.strip()
                if unit.name and not spec.startswith(unit.name + '.'):
                    spec = unit.name + '.' + spec
                self.add_line(spec)
            else:
                if 'feature' not in spec:
                    raise ValueError(f'Invalid feature specifier {spec}.')
                feat = spec['feature']
                ops = [k for k in spec if k.startswith('value')]
                if not ops:
                    self.add(unit[feat].exists())
                for key in ops:
                    val = spec[key]
                    neg = False
                    if '_' in key:
                        op = key.split('_')[1]
                    else:
                        neg = ('not' in key)
                        op = '='
                    if op.startswith('not'):
                        op = op[3:]
                        neg = True
                    if op == 'exist':
                        cond = unit[feat].exists()
                        neg = neg and not val
                    elif op not in op_lookup:
                        raise ValueError(f'Unknown constraint {key}.')
                    elif isinstance(val, list):
                        if not val:
                            continue
                        cond = Condition(unit[feat], val[0], op_lookup[op])
                        for v in val[1:]:
                            cond = Condition(cond,
                                             Condition(unit[feat], v, op_lookup[op]),
                                             'OR')
                    else:
                        cond = Condition(unit[feat], val, op_lookup[op])
                    if neg:
                        cond = ~cond
                    self.add(cond)
        if 'order' in pattern:
            unit.order = pattern['order']
        parent = pattern.get('parent')
        if parent:
            if parent not in self.name2unit:
                raise ValueError(f'No node named {parent} (refereced by {unit.name}).')
            self.add(unit.parent(self.name2unit[parent]))
        next = pattern.get('next')
        if next:
            if next not in self.name2unit:
                raise ValueError(f'No node named {next} (refereced by {unit.name}).')
            if not parent:
                raise ValueError(f'Adjacency constraint requires a named parent for {unit.name}.')
            nu = self.name2unit[next]
            self.add(nu.parent(parent) & (unit['meta:index'] + 1 == nu['meta:index']))
        for sq in pattern.get('subqueries', []):
            mn = sq.get('min', 1)
            if not isinstance(mn, int):
                mn = 1
            mx = sq.get('max')
            if mx is not None and not isinstance(mx, int):
                mx = None
            Q, _ = unit.subquery(min=mn, max=mx)
            Q.parse_query_dict(sq)

    @staticmethod
    def parse_query(db, query, order=None, type_map=None, feat_map=None):
        if isinstance(query, Query):
            return query
        Q = Query(db, type_map, feat_map)
        if isinstance(query, dict):
            Q.parse_query_dict(query, order)
        elif isinstance(query, str):
            for linenumber, line in enumerate(query.splitlines(), 1):
                Q.add_line(line, linenumber=linenumber)
        else:
            raise ValueError(f'Query must be dictionary or string, not {query.__type__.__name__}.')
        return Q

def search(db, query, order=None):
    Q = Query.parse_query(db, query, order)
    yield from Q.search()

class ResultTable:
    # TODO: subquery support
    def __init__(self, db, query, order=None, type_map=None, feat_map=None):
        self.db = db
        self.type_map = type_map or {}
        self.rev_type_map = {v:k for k, v in self.type_map.items()}
        self.feat_map = feat_map or {}
        self.rev_feat_map = {v:k for k, v in self.feat_map.items()}
        Q = Query.parse_query(db, query, order, self.rev_type_map, self.rev_feat_map)
        self.nodes = list(Q.search())
        self.features = []
        self.feature_names = {}
        for result in self.nodes:
            dct = {}
            for l in result.values():
                for uid in utils.as_list(l):
                    if isinstance(uid, dict):
                        continue
                    dct[uid] = {}
            self.features.append(dct)
        self.types = {u.name: utils.map_type(self.rev_type_map, u.type)
                      for u in Q.units}
        self.unit2results = defaultdict(list)
        for i, result in enumerate(self.nodes):
            for uid in result.values():
                for u in utils.as_list(uid):
                    if isinstance(u, dict):
                        continue
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
