#!/usr/bin/env python3

from rebabel_format.db import RBBLFile
from rebabel_format.parameters import Parameter, DBParameter, QueryParameter, process_parameters
import logging

ALL_PROCESSES = {}

class Process:
    name = None
    parameters = {}
    db = DBParameter(help='the database file to operate on')

    def __init__(self, conf, **kwargs):
        self.conf = conf
        self.other_args = kwargs
        self.parameter_values = process_parameters(self.parameters, conf, self.name, kwargs)
        self.logger = logging.getLogger('reBabel.' + (self.name or 'unnamed_process'))

    def __init_subclass__(cls, *args, **kwargs):
        global ALL_PROCESSES
        super.__init_subclass__(*args, **kwargs)
        if cls.name:
            if cls.name in ALL_PROCESSES:
                raise ValueError(f'Identifier {cls.name} is already used by another Process class.')
            ALL_PROCESSES[cls.name] = cls

    def run(self):
        pass

    def get_feature(self, unittype: str, feature: str):
        return self.db.get_feature(unittype, feature)

    @classmethod
    def help_text_epilog(cls):
        return None

    @classmethod
    def help_text(cls):
        import textwrap
        ret = str(cls.name)
        if cls.__doc__:
            ret += ': ' + cls.__doc__
        ret = textwrap.wrap(ret)
        ret += ['', 'Parameters:']
        for name, param in cls.parameters.items():
            ret += textwrap.wrap(f'{name}: {param.help_text()}',
                                 initial_indent='  ',
                                 subsequent_indent='    ')
        epilog = cls.help_text_epilog()
        if epilog:
            ret += ['', epilog]
        return '\n'.join(ret)

class SearchProcess(Process):
    query = QueryParameter()

    def get_value(self, result, spec):
        uid = result[spec['unit']]
        utype = self.db.get_unit_type(uid)
        fid, vtype = self.db.get_feature(utype, spec['feature'])
        return self.db.get_feature_value(uid, fid)

    def print_label(self, result, labels):
        for i, label in enumerate(labels):
            print(self.get_value(result, label),
                  end=(': ' if i+1 == len(labels) else ' '))

    def pre_search(self):
        pass

    def per_result(self, result):
        pass

    def post_search(self):
        pass

    def run(self):
        from rebabel_format.query import search
        self.pre_search()
        for result in search(self.db, self.query):
            self.per_result(result)
        self.post_search()
