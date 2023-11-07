#!/usr/bin/env python3

from ..db import RBBLFile

ALL_READERS = {}
ALL_DESCRIPTIONS = {}

class BaseReader:
    def __init__(self, db, user):
        self.known_feats = {}
        self.db = db
        self.user = user

    def ensure_feature(self, unittype, tier, feature, valuetype):
        key = (unittype, tier, feature)
        if key in self.known_feats:
            return self.known_feats[key]
        fid, typ = self.db.get_feature(unittype, tier, feature, error=False)
        if typ == valuetype:
            self.known_feats[key] = fid
            return fid
        elif typ is not None:
            raise ValueError(f'Feature {tier}:{feature} for {unittype} already exists with value type {typ}.')
        else:
            self.db.create_feature(unittype, tier, feature, valuetype)
            fid, typ = self.db.get_feature(unittype, tier, feature, error=False)
            self.known_feats[key] = fid
            return fid

    def create_unit(self, unittype, parent=None):
        uid = self.db.create_unit(unittype)
        if parent:
            self.db.set_parent(parent, uid)
        return uid

    def set_feature(self, uid, tier, name, value):
        self.db.set_feature(uid, tier, name, value, self.user)

    def read_file(self, fin):
        pass

    def open_file(self, pth):
        self.db.committing = False
        if self.db.current_time is None:
            self.db.current_time = self.db.now()
        return open(pth)

    def close_file(self, fin):
        fin.close()
        self.db.committing = True
        self.db.commit()

    def commit(self):
        self.db.committing = True
        self.db.commit()
        self.db.committing = False

class MetaReader(type):
    def __new__(cls, name, bases, attrs):
        global ALL_READERS, ALL_DESCRIPTIONS
        new_attrs = attrs.copy()
        ident = attrs.get('identifier')
        short_name = attrs.get('short_name')
        long_name = attrs.get('long_name')
        if ident in ALL_READERS:
            raise ValueError(f'Identifier {ident} is already used by another reader class.')
        for a, v in attrs.items():
            if a in ['identifier', 'short_name', 'long_name']:
                del new_attrs[a]
        ret = super(MetaReader, cls).__new__(cls, name, bases, new_attrs)
        if ident is not None:
            ALL_READERS[ident] = ret
            ALL_DESCRIPTIONS[ident] = (short_name, long_name)
        return ret

class Reader(BaseReader, metaclass=MetaReader):
    pass

def read_new(conf):
    from ..config import get_param, get_single_param
    import os
    mode = get_single_param(conf, 'import', 'mode')
    if mode not in ALL_READERS:
        raise ValueError(f'Unknown reader {mode}.')
    out_path = get_single_param(conf, 'import', 'db')
    db = RBBLFile(out_path)
    username = get_single_param(conf, 'import', 'username')
    username = username or os.environ.get('USER', 'import-script')
    r = ALL_READERS[mode](db, username)
    in_pths = get_single_param(conf, 'import', 'infiles')
    for pth in in_pths:
        fin = r.open_file(pth)
        r.read_file(fin)
        r.close_file(fin)
