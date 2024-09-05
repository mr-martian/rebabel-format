#!/usr/bin/env python3

from rebabel_format.db import RBBLFile
from rebabel_format.parameters import Parameter
import logging
from collections import defaultdict

ALL_READERS = {}

class ReaderError(Exception):
    pass

class MetaReader(type):
    def __new__(cls, name, bases, attrs):
        global ALL_READERS
        new_attrs = attrs.copy()
        ident = attrs.get('identifier')
        if ident in ALL_READERS:
            raise ValueError(f'Identifier {ident} is already used by another reader class.')
        if ident:
            new_attrs['logger'] = logging.getLogger('reBabel.reader.'+ident)

        parameters = {}
        for b in bases:
            parameters.update(getattr(b, 'parameters', {}))
        for attr, value in attrs.items():
            if isinstance(value, Parameter):
                parameters[attr] = value
                del new_attrs[attr]
        new_attrs['parameters'] = parameters

        ret = super(MetaReader, cls).__new__(cls, name, bases, new_attrs)
        if ident is not None:
            ALL_READERS[ident] = ret
        return ret

class Reader(metaclass=MetaReader):
    def __init__(self, db, user, conf, kwargs):
        self.db = db
        self.user = user

        self.conf = conf
        self.other_args = kwargs
        for name, parser in self.parameters.items():
            if name in kwargs:
                value = parser.process(name, kwargs[name])
            else:
                value = parser.extract(conf, 'import', name)
            setattr(self, name, value)

        self.known_feats = {}
        self.uids = {}
        self.all_ids = set()
        self.id_seq = []
        self.parents = {}
        self.relations = defaultdict(set)
        self.types = {}
        self.features = defaultdict(dict) # (tier, feature, type) => value

        self.filename = None
        self.location = None

        self.block_count = 0

        self.type_map = {}
        self.feature_map = {}

    def info(self, msg):
        try:
            self.logger.info(msg)
        except AttributeError:
            pass

    def warning(self, msg):
        try:
            self.logger.warn(msg)
        except AttributeError:
            pass

    def error(self, msg):
        prefix = ', '.join([x for x in [self.filename, self.location] if x])
        if prefix:
            prefix += ': '
        send = prefix + msg
        try:
            self.logger.error(send)
            send = ''
        except AttributeError:
            pass
        raise ReaderError(send)

    def set_mappings(self, type_map, feat_map):
        self.type_map = type_map
        for (fi, ti), (fo, to) in feat_map.items():
            self.feature_map[(fo, ti)] = fi

    def _check_name(self, name):
        if name not in self.all_ids:
            self.all_ids.add(name)
            self.id_seq.append(name)

    def set_type(self, unit_name, unit_type):
        self._check_name(unit_name)
        self.types[unit_name] = self.type_map.get(unit_type, unit_type)

    def set_parent(self, child_name, parent_name):
        self._check_name(parent_name)
        self._check_name(child_name)
        self.parents[child_name] = parent_name

    def add_relation(self, child_name, parent_name):
        self._check_name(parent_name)
        self._check_name(child_name)
        self.relations[child_name].add(parent_name)

    def set_feature(self, unit_name, tier, feature, ftype, value,
                    confidence=None):
        self._check_name(unit_name)
        if ftype == 'ref':
            self._check_name(value)
        else:
            self.db.check_type(ftype, value)
        self.features[unit_name][(tier, feature, ftype)] = (value, confidence)

    def finish_block(self, parent_if_missing=None, keep_uids=False):
        parent_type_if_missing = None
        if parent_if_missing is not None:
            parent_type_if_missing = self.db.get_unit_type(parent_if_missing)

        uids = self.uids.copy()
        for name in self.id_seq:
            if name in uids:
                continue
            if name not in self.types:
                self.error(f"Unit '{name}' has not been assigned a type.")
            uids[name] = self.db.create_unit(self.types[name], user=self.user)

        parents = []
        for name in self.id_seq:
            parent_name = self.parents.get(name)
            parent = uids.get(parent_name, parent_if_missing)
            if parent is None:
                continue
            parent_type = self.types.get(parent_name, parent_type_if_missing)
            parents.append(
                {'parent': parent, 'parent_type': parent_type,
                 'child': uids[name], 'child_type': self.types[name],
                 'isprimary': True, 'active': True, 'date': self.db.now()}
            )
            for rname in sorted(self.relations[name]):
                rid = uids[rname]
                rtype = self.types[rname]
                parents.append(
                    {'parent': rid, 'parent_type': rtype,
                     'child': uids[name], 'child_type': self.types[name],
                     'isprimary': False, 'active': True, 'date': self.db.now()}
                )
        self.db.cur.executemany(
            'INSERT INTO relations(parent, parent_type, child, child_type, isprimary, active, date) VALUES(:parent, :parent_type, :child, :child_type, :isprimary, :active, :date)',
            parents,
        )
        self.parents = {}
        self.relations = defaultdict(set)

        feature_ids = {}
        features = []
        for name in self.id_seq:
            for (tier, feature, ftype), (value, conf) in self.features[name].items():
                m_key = ((tier, feature), self.types[name])
                n_key = ((tier, feature), None)
                if m_key in self.feature_map:
                    tier, feature = self.feature_map[m_key]
                elif n_key in self.feature_map:
                    tier, feature = self.feature_map[n_key]
                key = (tier, feature, ftype, self.types[name])
                if key in feature_ids:
                    fid = feature_ids[key]
                else:
                    fid = self.ensure_feature(self.types[name], tier, feature, ftype)
                    feature_ids[key] = fid
                if ftype == 'ref':
                    value = uids[value]
                features.append({
                    'unit': uids[name], 'feature': fid, 'value': value,
                    'user': self.user, 'date': self.db.now(),
                    'confidence': conf,
                })
        self.db.cur.executemany(
            'INSERT INTO features(unit, feature, value, user, date, confidence) VALUES(:unit, :feature, :value, :user, :date, :confidence)',
            features,
        )
        self.features = defaultdict(dict)

        self.id_seq = []
        if keep_uids:
            self.uids = uids
        else:
            self.all_ids = set()
            self.types = {}
        self.block_count += 1

    def ensure_feature(self, unittype, tier, feature, valuetype):
        key = (unittype, tier, feature)
        if key in self.known_feats:
            return self.known_feats[key]
        fid, typ = self.db.get_feature(unittype, tier, feature, error=False)
        if typ == valuetype:
            self.known_feats[key] = fid
            return fid
        elif typ is not None:
            self.error(f'Feature {tier}:{feature} for {unittype} already exists with value type {typ}.')
        else:
            self.db.create_feature(unittype, tier, feature, valuetype)
            fid, typ = self.db.get_feature(unittype, tier, feature, error=False)
            self.known_feats[key] = fid
            return fid

    def create_unit(self, unittype, parent=None):
        uid = self.db.create_unit_with_features(unittype, [], self.user,
                                                parent=parent)
        return uid

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
            self.filename = pth
            fin = self.open_file(pth)
            self.read_file(fin)
            self.close_file(fin)

    @classmethod
    def help_text(cls):
        if not hasattr(cls, 'identifier'):
            return ''
        ret = [f'Identifier: {cls.identifier}']
        if hasattr(cls, 'short_name'):
            ret.append(f'Short name: {cls.short_name}')
        if hasattr(cls, 'long_name'):
            ret.append(f'Long name: {cls.long_name}')
        if hasattr(cls, 'format_specification'):
            ret.append(f'Specification: {cls.format_specification}')
        if cls.__doc__:
            import textwrap
            for piece in textwrap.dedent(cls.__doc__).split('\n\n'):
                ret.append('')
                ret += textwrap.wrap(piece)
        return '\n'.join(ret)

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

class LineReader(Reader):
    block_name = 'sentence'

    def is_boundary(self, line):
        return not line

    def reset(self):
        pass

    def end(self):
        try:
            self.finish_block()
        except ReaderError:
            pass

    def process_line(self, line):
        pass

    def read_file(self, fin):
        self.reset()
        block_error = False
        self.location = 'line 1'
        for linenumber, line in enumerate(fin, 1):
            if self.is_boundary(line.strip()):
                self.end()
                self.location = f'line {linenumber}'
                self.reset()
                block_error = False
            elif not block_error:
                try:
                    self.process_line(line.strip())
                except ReaderError:
                    self.logger.error(f'Unable to import {self.block_name} beginning on {self.location}.')
                    block_error = True
        self.end()
