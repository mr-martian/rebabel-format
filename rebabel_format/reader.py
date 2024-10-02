#!/usr/bin/env python3

from rebabel_format.db import RBBLFile, WhereClause
from rebabel_format.parameters import Parameter, process_parameters
from rebabel_format.query import ResultTable
import logging
from collections import defaultdict

ALL_READERS = {}

class ReaderError(Exception):
    pass

class Reader:
    identifier = None
    parameters = {}

    merge_on = Parameter(required=False, type=dict)

    def __init__(self, db, user, conf, kwargs):
        self.db = db
        self.user = user

        self.conf = conf
        self.other_args = kwargs
        self.parameter_values = process_parameters(self.parameters, conf, 'import', kwargs)

        self.known_feats = {}
        self.uids = {}
        self.all_ids = set()
        self.id_seq = []
        self.parents = {}
        self.relations = defaultdict(set)
        self.types = {}
        self.features = defaultdict(dict) # (feature, type) => value

        self.filename = None
        self.location = None

        self.block_count = 0

        self.type_map = {}
        self.feature_map = {}

        self.logger = logging.getLogger('reBabel.reader.'+(self.identifier or 'unnamed_reader'))

    def __init_subclass__(cls, *args, **kwargs):
        global ALL_READERS
        super().__init_subclass__(*args, **kwargs)
        if cls.identifier:
            if cls.identifier in ALL_READERS:
                raise ValueError(f'Identifier {cls.identifier} is already used by another Reader class.')
            ALL_READERS[cls.identifier] = cls

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warn(msg)

    def error(self, msg):
        prefix = ', '.join([x for x in [self.filename, self.location] if x])
        if prefix:
            prefix += ': '
        self.logger.error(prefix+msg)
        raise ReaderError()

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

    def set_feature(self, unit_name, feature: str, ftype: str, value,
                    confidence=None):
        self._check_name(unit_name)
        if ftype == 'ref':
            self._check_name(value)
        else:
            self.db.check_type(ftype, value)
        self.features[unit_name][(feature, ftype)] = (value, confidence)

    def _remap_features(self):
        new_feats = defaultdict(dict)
        for name, dct in self.features.items():
            for (feature, ftype), val in dct.items():
                m_key = (feature, self.types.get(name))
                n_key = (feature, None)
                feature = self.feature_map.get(m_key,
                                               self.feature_map.get(n_key, feature))
                new_feats[name][(feature, ftype)] = val
        self.features = new_feats

    def finish_block(self, parent_if_missing=None, keep_uids=False):
        parent_type_if_missing = None
        if parent_if_missing is not None:
            parent_type_if_missing = self.db.get_unit_type(parent_if_missing)

        self._remap_features()
        uids = self.uids.copy()

        # node names which have been merged and thus need feature setting
        # rather than feature creation
        is_merged = set()

        if self.merge_on:
            # 1. Gather the values to merge on
            # node type => feature value => [node name, ...]
            merge_values = {k: defaultdict(list) for k in self.merge_on}
            for name in self.id_seq:
                typ = self.types.get(name)
                if typ in merge_values:
                    for feat, val in self.features[name].items():
                        if feat[0] == self.merge_on[typ]:
                            merge_values[typ][val[0]].append(name)

            # 2. Query for existing nodes with matching features
            # node name => [feature value, ...]
            merge_possible = defaultdict(list)
            for typ, val_map in merge_values.items():
                table = ResultTable(self.db,
                                    {'N': {
                                        'type': typ,
                                        'features': [{
                                            'feature': self.merge_on[typ],
                                            'value': sorted(val_map.keys()),
                                        }],
                                    }})
                table.add_features('N', [self.merge_on[typ]])
                for nodes, features in table.results():
                    val = features[nodes['N']][self.merge_on[typ]]
                    for name in val_map[val]:
                        merge_possible[name].append(nodes['N'])

            # 3. Extract all relations in file and DB
            child_names = defaultdict(list)
            all_merge = []
            for name in self.id_seq:
                if name not in merge_possible:
                    continue
                all_merge += merge_possible[name]
                if name not in self.parents:
                    continue
                if self.parents[name] not in merge_possible:
                    # TODO: what is parent is in self.uids?
                    del merge_possible[name]
                child_names[self.parents[name]].append(name)
            self.db.execute_clauses('SELECT parent, child FROM relations',
                                    WhereClause('child', all_merge),
                                    WhereClause('parent', all_merge),
                                    WhereClause('active', True),
                                    WhereClause('isprimary', True))
            child_ids = defaultdict(list)
            for p, c in self.db.cur.fetchall():
                child_ids[p].append(c)

            # 4. Remove correspondences which don't match existing
            # parent-child links
            todo = sorted(merge_possible.keys())
            while todo:
                next_todo = []
                for name in todo:
                    pc = set()
                    for n in merge_possible[name]:
                        pc.update(child_ids[n])
                    for ch in child_names[name]:
                        update = set(merge_possible[ch]) & pc
                        if len(update) < len(merge_possible[ch]):
                            merge_possible[ch] = sorted(update)
                            next_todo.append(ch)
                todo = next_todo

            # 5. Make the correspondences
            for name, ids in merge_possible.items():
                if ids:
                    uids[name] = ids[0]
                    is_merged.add(name)

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
            'INSERT OR IGNORE INTO relations(parent, parent_type, child, child_type, isprimary, active, date) VALUES(:parent, :parent_type, :child, :child_type, :isprimary, :active, :date)',
            parents,
        )
        self.parents = {}
        self.relations = defaultdict(set)

        feature_ids = {}
        features = []
        merge_features = []
        for name in self.id_seq:
            for (feature, ftype), (value, conf) in self.features[name].items():
                key = (feature, ftype, self.types[name])
                if key in feature_ids:
                    fid = feature_ids[key]
                else:
                    fid = self.ensure_feature(self.types[name], feature, ftype)
                    feature_ids[key] = fid
                if ftype == 'ref':
                    value = uids[value]
                dct = {
                    'unit': uids[name], 'feature': fid, 'value': value,
                    'user': self.user, 'date': self.db.now(),
                    'confidence': conf,
                }
                if name in is_merged:
                    merge_features.append(dct)
                else:
                    features.append(dct)
        if features:
            self.db.cur.executemany(
                'INSERT INTO features(unit, feature, value, user, date, confidence) VALUES(:unit, :feature, :value, :user, :date, :confidence)',
                features,
            )
        if merge_features:
            self.db.cur.executemany(
                'UPDATE features SET value = :value, user = :user, confidence = :confidence, date = :date WHERE unit = :unit AND feature = :feature',
                merge_features,
            )
            self.db.cur.executemany(
                'INSERT OR IGNORE INTO features(unit, feature, value, user, confidence, date) VALUES(:unit, :feature, :value, :user, :confidence, :date)',
                merge_features,
            )
        self.features = defaultdict(dict)

        self.id_seq = []
        if keep_uids:
            self.uids = uids
        else:
            self.all_ids = set()
            self.types = {}
        self.block_count += 1

    def ensure_feature(self, unittype, feature, valuetype):
        key = (unittype, feature)
        if key in self.known_feats:
            return self.known_feats[key]
        fid, typ = self.db.get_feature(unittype, feature, error=False)
        if typ == valuetype:
            self.known_feats[key] = fid
            return fid
        elif typ is not None:
            self.error(f'Feature {feature} for {unittype} already exists with value type {typ}.')
        else:
            self.db.create_feature(unittype, feature, valuetype)
            fid, typ = self.db.get_feature(unittype, feature, error=False)
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
    include_boundaries = False

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
                if not self.include_boundaries:
                    continue
            if not block_error:
                try:
                    self.process_line(line.strip())
                except ReaderError:
                    self.logger.error(f'Unable to import {self.block_name} beginning on {self.location}.')
                    block_error = True
        self.end()
