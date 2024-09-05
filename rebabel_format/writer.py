#!/usr/bin/env python3

from rebabel_format.db import RBBLFile
from rebabel_format.config import parse_feature
from rebabel_format.parameters import Parameter

ALL_WRITERS = {}

class MetaWriter(type):
    def __new__(cls, name, bases, attrs):
        global ALL_WRITERS
        new_attrs = attrs.copy()
        ident = attrs.get('identifier')
        if ident in ALL_WRITERS:
            raise ValueError(f'Identifier {ident} is already used by another writer class.')

        parameters = {}
        for b in bases:
            parameters.update(getattr(b, 'parameters', {}))
        for attr, value in attrs.items():
            if isinstance(value, Parameter):
                parameters[attr] = value
                del new_attrs[attr]
        new_attrs['parameters'] = parameters

        ret = super(MetaWriter, cls).__new__(cls, name, bases, new_attrs)
        if ident is not None:
            ALL_WRITERS[ident] = ret
        return ret

class Writer(metaclass=MetaWriter):
    query = {}
    query_order = []

    def __init__(self, db, type_map, feat_map, conf, kwargs):
        self.db = db
        self.conf = conf
        self.other_args = kwargs
        for name, parser in self.parameters.items():
            if name in kwargs:
                value = parser.process(name, kwargs[name])
            else:
                value = parser.extract(conf, 'export', name)
            setattr(self, name, value)
        if self.query:
            self.pre_query()
            from rebabel_format.query import ResultTable
            self.table = ResultTable(self.db, self.query, self.query_order,
                                     type_map=type_map, feat_map=feat_map)

    def pre_query(self):
        pass

    def write(self, fout):
        pass
