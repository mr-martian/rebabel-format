#!/usr/bin/env python3

from rebabel_format.config import get_single_param, parse_mappings
from dataclasses import dataclass
from typing import Any, Optional, Sequence

@dataclass
class Parameter:
    required: bool = True
    default: Any = None
    type: type = None
    help: str = 'a parameter'
    choices: Optional[Sequence[Any]] = None

    name = None

    def __post_init__(self):
        if self.default is not None:
            self.required = False

    def __set_name__(self, owner, name):
        # make a copy so that we don't end up sharing a mutable dictionary
        # between all the different subclasses
        dct = {}
        if hasattr(owner, 'parameters'):
            dct.update(owner.parameters)
        dct[name] = self
        owner.parameters = dct
        self.name = name

    def __get__(self, instance, owner):
        return instance.parameter_values[self.name]

    def process(self, name, value):
        if value is None:
            if self.required:
                raise ValueError(f"Missing parameter '{name}'.")
            elif self.default:
                return self.default
        else:
            if self.type and not isinstance(value, self.type):
                raise ValueError(f"Parameter '{name}' should be {self.type} but it is {type(value)}.")
            if self.choices and value not in self.choices:
                raise ValueError(f"Parameter '{name}' should be one of {', '.join(map(str, self.choices))}.")
        return value

    def extract(self, conf, action, attribute):
        value = get_single_param(conf, action, attribute)
        return self.process(attribute, value)

    def help_text(self):
        paren = []
        if self.type:
            paren.append(self.type.__name__)
        if self.required:
            paren.append('required')
        if self.default:
            paren.append(f'default: {self.default}')
        ret = self.help
        if paren:
            ret += f' ({"; ".join(paren)})'
        return ret

def process_parameters(parameters, conf, conf_prefix, kwargs):
    ret = {}
    for name, parser in parameters.items():
        if name in kwargs:
            ret[name] = parser.process(name, kwargs[name])
        else:
            ret[name] = parser.extract(conf, conf_prefix, name)
    return ret

@dataclass
class DBParameter(Parameter):
    type: type = str

    def process(self, name, value):
        val = super().process(name, value)
        from rebabel_format.db import RBBLFile
        return RBBLFile(val)

@dataclass
class QueryParameter(Parameter):
    type: type = dict
    required: bool = True

    def process(self, name, value):
        val = super().process(name, value)
        nodes = set()
        for name, item in val.items():
            if not isinstance(item, dict) or 'type' not in item:
                continue
            nodes.add(name)
            if 'print' in item and not isinstance(item['print'], list):
                item['print'] = [item['print']]
        if not nodes:
            raise ValueError('Query contains no valid nodes.')
        # TODO: check parent etc
        return val

@dataclass
class MappingParameter(Parameter):
    type: type = list

    def process(self, name, value):
        val = super().process(name, value)
        return parse_mappings(val)

@dataclass
class UsernameParameter(Parameter):
    required: bool = False
    type: type = str

    def process(self, name, value):
        val = super().process(name, value)
        if val is None:
            import os
            return os.environ.get('USER', 'script')
        return val
