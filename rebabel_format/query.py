#!/usr/bin/env python3

from .db import RBBLFile
from .config import parse_feature

from collections import defaultdict
import re

class FeatureQuery:
    like_escape = re.compile('([%_$])')
    def __init__(self, featid, table, value=None, operator=None):
        self.featid = featid
        self.table = table
        self.value = value
        self.operator = operator
    def query(self, unitids=None):
        ret = f'SELECT f.unit, f.value FROM {self.table} f WHERE f.active = ? AND f.user IS NOT NULL AND f.feature = ?'
        params = [True, self.featid]
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
            fq = FeatureQuery(fid, ftyp+'_features', spec[v[0]], v[0])
        else:
            fq = FeatureQuery(fid, ftyp+'_features')
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
        qr = 'SELECT u.id FROM units u WHERE type = ? AND active = ?'
        params = [self.unittype, True]
        for f in self.features:
            q, p = f.query('u.id')
            qr += f' AND EXISTS ({q})'
            params += p
        self.db.cur.execute(qr, params)
        ids = [x[0] for x in self.db.cur.fetchall()]
        for f in self.features:
            ids = self.check(f, ids)
        return ids

def search(db, query):
    units = {}
    rels = [] # [(parent, child), ...]
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
    # TODO: I expect there's some more intelligent way to do this part of the
    # querying if we pre-compute the right order to search for certain things
    # and then work them into the unit queries. Unfortunately, by ensuring
    # that the code below will return things in a consistent order, I've
    # probably doomed us to forever maintain that order per Hyrum's Law.
    # DGS 2023-11-21
    intersect = {}
    for parent, child in rels:
        q1 = ', '.join(['?']*len(units[parent]))
        q2 = ', '.join(['?']*len(units[child]))
        db.cur.execute(
            f'SELECT parent, child FROM relations WHERE parent IN ({q1}) AND child IN ({q2}) AND active = ?',
            units[parent] + units[child] + [True],
        )
        key = (parent, child)
        pairs = set(db.cur.fetchall())
        if not pairs:
            return
        if key in intersect:
            pairs = intersect[key].intersection(pairs)
        intersect[key] = pairs
    seq = sorted(units.keys())
    def combine(cur):
        nonlocal seq, units, intersect
        if len(cur) == len(seq):
            yield dict(zip(seq, cur))
        else:
            k = seq[len(cur)]
            u = set(units[k])
            for i, n in enumerate(seq[:len(cur)]):
                if (k, n) in intersect:
                    u = set([x[0] for x in intersect[(k, n)] if x[1] == cur[i]])
                if (n, k) in intersect:
                    u = set([x[1] for x in intersect[(n, k)] if x[0] == cur[i]])
            for i in sorted(u):
                yield from combine(cur + [i])
    yield from combine([])

def search_conf(conf):
    from .config import get_single_param
    if 'query' not in conf:
        raise ValueError('Missing query')
    db = RBBLFile(get_single_param(conf, 'query', 'db'))
    print_feats = {}
    lab_width = 0
    for name in conf['query']:
        if not isinstance(conf['query'][name], dict):
            continue
        pr = conf['query'][name].get('print', [])
        typ = conf['query'][name].get('type')
        if not isinstance(pr, list):
            pr = [pr]
        if typ is None:
            continue
        print_feats[name] = []
        for p in pr:
            t, f = parse_feature(p)
            i, vtyp = db.get_feature(typ, t, f, error=True)
            lab = t+':'+f
            lab_width = max(lab_width, len(lab))
            print_feats[name].append((lab, i, vtyp))
    for n, result in enumerate(search(db, conf['query']), 1):
        print('Result', n)
        for name, uid in sorted(result.items()):
            print(name, uid)
            for lab, fid, ftyp in print_feats.get(name, []):
                v = str(db.get_feature_value(uid, fid, ftyp))
                print('\t'+lab.ljust(lab_width+3)+'\t'+v)
        print('')
