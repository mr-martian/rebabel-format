#!/usr/bin/env python3

from rebabel_format.db import RBBLFile
from rebabel_format.config import parse_feature
from rebabel_format.parameters import Parameter, DBParameter, QueryParameter
import logging

ALL_PROCESSES = {}

class MetaProcess(type):
    def __new__(cls, name, bases, attrs):
        global ALL_PROCESSES
        new_attrs = attrs.copy()
        proc_name = attrs.get('name')
        parameters = {}
        for b in bases:
            parameters.update(getattr(b, 'parameters', {}))
        for attr, value in attrs.items():
            if isinstance(value, Parameter):
                parameters[attr] = value
                del new_attrs[attr]
        new_attrs['parameters'] = parameters
        if proc_name:
            new_attrs['logger'] = logging.getLogger('reBabel.'+proc_name)
        ret = super(MetaProcess, cls).__new__(cls, name, bases, new_attrs)
        if proc_name is not None:
            ALL_PROCESSES[proc_name] = ret
        return ret

class Process(metaclass=MetaProcess):
    name = None
    db = DBParameter(help='the database file to operate on')

    def __init__(self, conf, **kwargs):
        self.conf = conf
        self.other_args = kwargs
        for name, parser in self.parameters.items():
            if name in kwargs:
                value = parser.process(name, kwargs[name])
            else:
                value = parser.extract(conf, self.name, name)
            setattr(self, name, value)

    def run(self):
        pass

    def get_feature(self, unittype, spec):
        tier, feature = parse_feature(spec)
        return self.db.get_feature(unittype, tier, feature)

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
        fid, vtype = self.db.get_feature(utype, spec['tier'], spec['feature'])
        return self.db.get_feature_value(uid, fid)

    def print_label(self, result):
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
