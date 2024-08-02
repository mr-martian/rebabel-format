#!/usr/bin/env python3

import sqlite3 as sql
import datetime
import os.path
import functools

def load_schema():
    try:
        from importlib.resources import files
    except ImportError:
        from importlib_resources import files
    return files().joinpath('schema.sql').read_text()

sql.register_adapter(datetime.datetime, lambda d: d.isoformat())
sql.register_converter('datetime', lambda b: datetime.datetime.fromisoformat(b.decode()))

sql.register_adapter(bool, lambda bl: b'1' if bl else b'0')
sql.register_converter('bool', lambda b: False if b == b'0' else True)

class RBBLFile:
    def commit_group(fn):
        @functools.wraps(fn)
        def _fn(self, *args, **kwargs):
            time_was = self.current_time
            com_was = self.committing
            self.current_time = self.now()
            self.committing = False
            ret = fn(self, *args, **kwargs)
            self.current_time = time_was
            self.committing = com_was
            self.commit()
            return ret
        return _fn

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
            return datetime.datetime.now()
        else:
            return self.current_time

    def commit(self):
        if self.committing:
            self.con.commit()

    def set_time(self, dt):
        self.current_time = dt

    @commit_group
    def insert(self, table, *args):
        cols = []
        qs = []
        vals = []
        for c, v in args:
            cols.append(c)
            qs.append('?')
            vals.append(v)
        q = f'INSERT INTO {table}({", ".join(cols)}) VALUES({", ".join(qs)})'
        self.cur.execute(q, vals)

    @commit_group
    def ensure_type(self, typename: str) -> bool:
        '''Ensure that a unit type named `typename` exists.
        return whether it was created.'''
        ex = self.first('SELECT * FROM tiers WHERE unittype = ?', typename)
        if ex is None:
            self.insert('tiers', ('tier', 'meta'), ('feature', 'active'),
                        ('unittype', typename), ('valuetype', 'bool'))
            return True
        return False

    @commit_group
    def create_feature(self, unittype, tier, feature, valuetype):
        if valuetype not in ['int', 'bool', 'str', 'ref']:
            raise ValueError('Unknown value type %s.' % valuetype)
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

    @commit_group
    def create_unit(self, unittype: str, user=None) -> int:
        self.ensure_type(unittype)
        meta, _ = self.get_feature(unittype, 'meta', 'active')
        self.insert('units', ('type', unittype), ('created', self.now()),
                    ('modified', self.now()), ('active', True))
        uid = self.cur.lastrowid
        if user:
            self.insert('bool_features', ('unit', uid), ('feature', meta),
                        ('value', True), ('date', self.now()), ('active', True),
                        ('user', user))
        else:
            self.insert('bool_features', ('unit', uid), ('feature', meta),
                        ('value', False), ('date', self.now()), ('active', True))
        return uid

    @commit_group
    def create_unit_with_features(self, unittype: str, feats, user, parent=None) -> int:
        uid = self.create_unit(unittype, user)
        for tier, feat, val in feats:
            fid, typ = self.get_feature(unittype, tier, feat, error=True)
            self.check_type(typ, val)
            self.insert(f'{typ}_features', ('unit', uid), ('feature', fid),
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

    @commit_group
    def set_feature(self, unitid: int, tier: str, feature: str, value,
                    user: str, confidence: int = 1):
        unittype = self.get_unit_type(unitid)
        fid, typ = self.get_feature(unittype, tier, feature, error=True)
        self.check_type(typ, value)
        self.insert('%s_features' % typ, ('unit', unitid), ('feature', fid),
                    ('value', value), ('user', user), ('confidence', confidence),
                    ('date', self.now()), ('active', True))

    @commit_group
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
        tab = '%s_features' % typ
        if not normalize:
            total = 1
        for v, p in values:
            self.insert(tab, ('unit', unitid), ('feature', fid), ('value', v),
                        ('date', self.now()), ('probability', p/total),
                        ('active', True))

    @commit_group
    def rem_parent(self, parent: int, child: int, primary_only=False):
        qr = 'UPDATE relations SET active = ? WHERE parent = ? AND child = ?'
        args = [False, parent, child]
        if primary_only:
            qr += ' AND isprimary = ?'
            args.append(True)
        self.cur.execute(qr, args)

    @commit_group
    def set_parent(self, parent: int, child: int, primary=True, clear=True):
        ptyp = self.get_unit_type(parent)
        ctyp = self.get_unit_type(child)
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

    def get_unit_features(self, unitid: int, features):
        plc = ', '.join(['?']*len(features))
        ret = {}
        for typ in ['bool', 'int', 'str', 'ref']:
            self.cur.execute(
                f'SELECT feature, value FROM {typ}_features WHERE unit = ? AND active = ? AND feature IN ({plc}) AND user IS NOT NULL',
                [unitid, True] + features,
            )
            for f, v in self.cur.fetchall():
                ret[f] = v
        return ret

    def get_feature_value(self, unitid: int, featid: int, feattype: str):
        if feattype not in ['bool', 'int', 'str', 'ref']:
            raise ValueError(f'Unknown value type {feattype}.')
        ret = self.first(f'SELECT value FROM {feattype}_features WHERE unit = ? AND active = ? AND feature = ? AND user IS NOT NULL', unitid, True, featid)
        if ret is not None:
            ret = ret[0]
        return ret

    def get_feature_values(self, units, featid, feattype: str):
        if feattype not in ['bool', 'int', 'str', 'ref']:
            raise ValueError(f'Unknown value type {feattype}.')
        plc = ', '.join(['?']*len(units))
        self.cur.execute(f'SELECT unit, value FROM {feattype}_features WHERE unit IN ({plc}) AND active = ? AND feature = ? AND user IS NOT NULL',
                         units + [True, featid])
        ret = self.cur.fetchall()
        if not ret:
            return {}
        else:
            return dict(ret)
