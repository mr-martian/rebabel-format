#!/usr/bin/env python3

from ..db import RBBLFile
import logging
from collections import defaultdict

ALL_READERS = {}
ALL_DESCRIPTIONS = {}

class BaseReader:
    def __init__(self, db, user):
        self.known_feats = {}
        self.db = db
        self.user = user
        self.features = defaultdict(dict)

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
        uid = self.db.create_unit_with_features(unittype, [], self.user,
                                                parent=parent)
        #if parent:
        #    self.db.set_parent(parent, uid)
        return uid

    def create_unit_with_features(self, unittype, features, parent=None):
        return self.db.create_unit_with_features(unittype, features, self.user,
                                                 parent=parent)

    def set_feature(self, uid, tier, name, value):
        self.db.set_feature_new(uid, tier, name, value, self.user)

    def prepare_feature(self, uid, tier, name, value):
        if (tier, name) in self.known_feats:
            fid = self.known_feats[(tier, name)]
        else:
            unittype = self.db.get_unit_type(uid)
            fid, typ = self.db.get_feature(unittype, tier, name, error=True)
            self.known_feats[(tier, name)] = fid
        self.features[uid][fid] = value

    def commit_features(self):
        args = []
        for uid, dct in self.features.items():
            for fid, value in dct.items():
                args.append({'unit': uid, 'feature': fid, 'value': value,
                             'user': self.user, 'confidence': 1,
                             'date': self.db.now()})
        self.db.cur.executemany(
            'INSERT INTO features(unit, feature, value, user, confidence, date) VALUES(:unit, :feature, :value, :user, :confidence, :date)',
            args,
        )
        self.features = defaultdict(dict)

    def read_file(self, fin):
        pass

    def open_file(self, pth):
        return open(pth)

    def close_file(self, fin):
        fin.close()

    def commit(self):
        self.db.committing = True
        self.db.commit()
        self.db.committing = False

    def read(self, pth):
        with self.db.transaction():
            fin = self.open_file(pth)
            self.read_file(fin)
            self.close_file(fin)

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
        if ident:
            new_attrs['logger'] = logging.getLogger('reBabel.reader.'+ident)
        ret = super(MetaReader, cls).__new__(cls, name, bases, new_attrs)
        if ident is not None:
            ALL_READERS[ident] = ret
            ALL_DESCRIPTIONS[ident] = (short_name, long_name)
        return ret

class Reader(BaseReader, metaclass=MetaReader):
    pass

class XMLReader(Reader):
    def open_file(self, pth):
        import xml.etree.ElementTree as ET
        return ET.parse(pth).getroot()

    def close_file(self, fin):
        pass

class JSONReader(Reader):
    def open_file(self, pth):
        import json
        with open(pth) as fin:
            return json.load(fin)

    def close_file(self, fin):
        pass
