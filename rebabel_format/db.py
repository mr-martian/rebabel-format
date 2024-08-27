#!/usr/bin/env python3

import sqlite3 as sql
import datetime
import os.path
import itertools
import contextlib
from collections import defaultdict

def load_schema():
    try:
        from importlib.resources import files
    except ImportError:
        from importlib_resources import files
    return files('rebabel_format').joinpath('schema.sql').read_text()

sql.register_adapter(datetime.datetime, lambda d: d.isoformat())
sql.register_converter('datetime', lambda b: datetime.datetime.fromisoformat(b.decode()))

sql.register_adapter(bool, lambda bl: b'1' if bl else b'0')
sql.register_converter('bool', lambda b: False if b == b'0' else True)

class RBBLFile:
    @contextlib.contextmanager
    def transaction(self):
        time_was = self.current_time
        com_was = self.committing
        self.current_time = self.now()
        self.committing = False
        try:
            yield None
        finally:
            self.current_time = time_was
            self.committing = com_was
            self.commit()

    def __init__(self, pth, create=True):
        self.path = pth
        if not os.path.exists(pth):
            if create:
                self.con = sql.connect(pth, detect_types=sql.PARSE_DECLTYPES)
                schema = load_schema()
                self.con.executescript(schema)
            else:
                raise FileNotFoundError('Database file %s does not exist.' % pth)
        else:
            self.con = sql.connect(pth, detect_types=sql.PARSE_DECLTYPES)
        self.cur = self.con.cursor()
        self.current_time = None
        self.committing = True

    def first(self, qr, *args):
        self.cur.execute(qr + ' LIMIT 1', args)
        return self.cur.fetchone()

    def now(self):
        if self.current_time is None:
            return datetime.datetime.now().isoformat()
        else:
            return self.current_time

    def commit(self):
        if self.committing:
            self.con.commit()

    def set_time(self, dt):
        self.current_time = dt

    def insert(self, table, *args, **kwargs):
        cols = []
        qs = []
        vals = []
        for c, v in itertools.chain(args, kwargs.items()):
            cols.append(c)
            qs.append('?')
            vals.append(v)
        q = f'INSERT INTO {table}({", ".join(cols)}) VALUES({", ".join(qs)})'
        self.cur.execute(q, vals)

    def ensure_type(self, typename: str) -> bool:
        '''Ensure that a unit type named `typename` exists.
        return whether it was created.'''
        ex = self.first('SELECT * FROM tiers WHERE unittype = ?', typename)
        if ex is None:
            self.insert('tiers', ('tier', 'meta'), ('feature', 'active'),
                        ('unittype', typename), ('valuetype', 'bool'))
            return True
        return False

    def create_feature(self, unittype, tier, feature, valuetype):
        if valuetype not in ['int', 'bool', 'str', 'ref']:
            raise ValueError('Unknown value type %s.' % valuetype)
        with self.transaction():
            self.ensure_type(unittype)
            # TODO: check if already exists
            self.insert('tiers', ('tier', tier), ('feature', feature),
                        ('unittype', unittype), ('valuetype', valuetype))

    def get_feature(self, unittype, tier, feature, error=False):
        ret = self.first('SELECT id, valuetype FROM tiers WHERE unittype = ? AND tier = ? AND feature = ?', unittype, tier, feature)
        if ret is None:
            if error:
                raise ValueError('Feature %s:%s does not exist for unit type %s.' % (tier, feature, unittype))
            return None, None
        return ret

    def create_unit(self, unittype: str, user=None) -> int:
        with self.transaction():
            self.ensure_type(unittype)
            meta, _ = self.get_feature(unittype, 'meta', 'active')
            self.insert('units', ('type', unittype), ('created', self.now()),
                        ('modified', self.now()), ('active', True))
            uid = self.cur.lastrowid
            self.insert('features', ('unit', uid), ('feature', meta),
                        ('value', True), ('date', self.now()), ('user', user))
            return uid

    def create_unit_with_features(self, unittype: str, feats, user, parent=None) -> int:
        with self.transaction():
            uid = self.create_unit(unittype, user)
            for tier, feat, val in feats:
                fid, typ = self.get_feature(unittype, tier, feat, error=True)
                self.check_type(typ, val)
                self.insert('features', ('unit', uid), ('feature', fid),
                            ('value', val), ('user', user), ('confidence', 1),
                            ('date', self.now()), ('active', True))
            if parent:
                ptyp = self.get_unit_type(parent)
                self.insert('relations', ('parent', parent), ('parent_type', ptyp),
                            ('child', uid), ('child_type', unittype),
                            ('isprimary', True), ('active', True),
                            ('date', self.now()))
            return uid

    def get_unit_type(self, unitid: int) -> str:
        ret = self.first('SELECT type FROM units WHERE id = ?', unitid)
        if ret is None:
            raise ValueError('Unit %s does not exist.' % unitid)
        return ret[0]

    def check_type(self, typename, value):
        if typename == 'str' and not isinstance(value, str):
            raise ValueError()
        elif typename == 'bool' and not isinstance(value, bool):
            raise ValueError()
        elif typename in ['int', 'ref'] and not isinstance(value, int):
            raise ValueError()

    def set_feature(self, unitid: int, tier: str, feature: str, value,
                    user: str, confidence: int = 1):
        unittype = self.get_unit_type(unitid)
        fid, typ = self.get_feature(unittype, tier, feature, error=True)
        self.check_type(typ, value)
        params = {'unit': unitid, 'feature': fid, 'value': value,
                  'user': user, 'confidence': confidence, 'date': self.now()}
        with self.transaction():
            self.cur.execute(
                'UPDATE features SET value = :value, user = :user, confidence = :confidence, date = :date WHERE unit = :unit AND feature = :feature',
                params,
            )
            self.cur.execute(
                'INSERT OR IGNORE INTO features(unit, feature, value, user, confidence, date) VALUES(:unit, :feature, :value, :user, :confidence, :date)',
                params,
            )

    def set_feature_dist(self, unitid: int, tier: str, feature: str,
                         values, normalize=True):
        if len(values) == 0:
            raise ValueError('The list of values must be non-empty.')
        unittype = self.get_unit_type(unitid)
        fid, typ = self.get_feature(unittype, tier, feature, error=True)
        total = 0.0
        for v, p in values:
            self.check_type(typ, v)
            if p <= 0:
                raise ValueError('Probabilities must be positive.')
            total += p
        if not normalize:
            total = 1
        with self.transaction():
            for v, p in values:
                self.insert('suggestions', ('unit', unitid), ('feature', fid),
                            ('value', v), ('date', self.now()),
                            ('probability', p/total), ('active', True))

    def rem_parent(self, parent: int, child: int, primary_only=False):
        qr = 'UPDATE relations SET active = ? WHERE parent = ? AND child = ?'
        args = [False, parent, child]
        if primary_only:
            qr += ' AND isprimary = ?'
            args.append(True)
        self.cur.execute(qr, args)

    def set_parent(self, parent: int, child: int, primary=True, clear=True):
        ptyp = self.get_unit_type(parent)
        ctyp = self.get_unit_type(child)
        with self.transaction():
            if primary or clear:
                self.rem_parent(parent, child, primary_only=(not clear))
            self.insert('relations', ('parent', parent), ('parent_type', ptyp),
                        ('child', child), ('child_type', ctyp),
                        ('isprimary', primary), ('active', True),
                        ('date', self.now()))

    def get_parent(self, uid: int):
        ret = self.first('SELECT parent FROM relations WHERE child = ? AND isprimary = ? AND active = ?', uid, True, True)
        if ret:
            ret = ret[0]
        return ret

    def get_all_features(self):
        self.cur.execute('SELECT * FROM tiers')
        return self.cur.fetchall()

    def get_units(self, unittype: str, parent=None):
        if parent is None:
            self.cur.execute(
                'SELECT id FROM units WHERE type = ? AND active = ?',
                (unittype, True),
            )
        else:
            self.cur.execute(
                'SELECT child FROM relations WHERE parent = ? AND child_type = ? AND active = ? AND isprimary = ?',
                (parent, unittype, True, True),
            )
        return [x[0] for x in self.cur.fetchall()]

    def get_children(self, units: list, child_type: str):
        plc = ', '.join(['?']*len(units))
        self.cur.execute(
            f'SELECT parent, child FROM relations WHERE parent IN ({plc}) AND child_type = ? AND active = ? AND isprimary = ?',
            units + [child_type, True, True],
        )
        ret = defaultdict(list)
        for parent, child in self.cur.fetchall():
            ret[parent].append(child)
        return ret

    def get_unit_features(self, unitid: int, features):
        plc = ', '.join(['?']*len(features))
        self.cur.execute(
            f'SELECT feature, value FROM features WHERE unit = ? AND feature IN ({plc})',
            [unitid] + features,
        )
        return dict(self.cur.fetchall())

    def get_feature_value(self, unitid: int, featid: int):
        ret = self.first('SELECT value FROM features WHERE unit = ? AND feature = ?', unitid, featid)
        if ret is not None:
            ret = ret[0]
        return ret

    def get_feature_values(self, units, featid):
        plc = ', '.join(['?']*len(units))
        self.cur.execute(f'SELECT unit, value FROM features WHERE unit IN ({plc}) AND feature = ?',
                         units + [featid])
        ret = self.cur.fetchall()
        if not ret:
            return {}
        else:
            return dict(ret)

class Unit:
    FeatureCache = {} # (tier, feature, type) => (id, valuetype)

    def __init__(self, db, id):
        self._db = db
        self._id = id
        self._type = self._db.get_unit_type(self._id)
        self._features = {}

    @property
    def id(self):
        return self._id

    @property
    def type(self):
        return self._type

    def _get_feature(self, key):
        tier, feature, id, valuetype = None, None, None, None
        if isinstance(key, tuple) and len(key) == 2:
            tier, feature = key
        elif isinstance(key, str) and ':' in key:
            tier, feature = key.split(':', 1)
        elif isinstance(key, int):
            id = key
        else:
            raise ValueError(f"Invalid feature identifier '{key}'.")
        if tier and feature:
            cache_key = (tier, feature, self._type)
            if cache_key in Unit.FeatureCache:
                id, valuetype = Unit.FeatureCache[cache_key]
            else:
                id, valuetype = self._db.get_feature(self._type, tier, feature,
                                                     error=True)
                Unit.FeatureCache[cache_key] = (id, valuetype)
        return id, valuetype

    def __getitem__(self, key):
        id, valuetype = self._get_feature(key)
        if id in self._features:
            value = self._features[id]
        else:
            value = self._db.get_feature_value(self._id, id)
            self._features[id] = value
        if valuetype == 'ref':
            return Unit(self._db, value)
        else:
            return value
