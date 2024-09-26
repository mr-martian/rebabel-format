#!/usr/bin/env python3

from rebabel_format.db import RBBLFile
from rebabel_format.parameters import Parameter, process_parameters

ALL_WRITERS = {}

class Writer:
    identifier = None
    parameters = {}
    query = {}
    query_order = []

    def __init__(self, db, type_map, feat_map, conf, kwargs,
                 query_updates=None):
        self.db = db
        self.conf = conf
        self.other_args = kwargs
        self.parameter_values = process_parameters(self.parameters, conf, 'export', kwargs)
        if self.query:
            self.pre_query()
            if query_updates:
                for k, v in query_updates.items():
                    if k in self.query:
                        self.query[k].update(v)
            from rebabel_format.query import ResultTable
            self.table = ResultTable(self.db, self.query, self.query_order,
                                     type_map=type_map, feat_map=feat_map)

    def __init_subclass__(cls, *args, **kwargs):
        global ALL_WRITERS
        super.__init_subclass__(*args, **kwargs)
        if cls.identifier:
            if cls.identifier in ALL_WRITERS:
                raise ValueError(f'Identifier {cls.identifier} is already used by another Writer class.')
            ALL_WRITERS[cls.identifier] = cls

    def pre_query(self):
        pass

    def write(self, fout):
        pass
