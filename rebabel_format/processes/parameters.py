#!/usr/bin/env python3

from ..config import get_single_param, parse_feature

class Parameter:
    def __init__(self, *args, required=True, default=None, type=None, **kwargs):
        self.required = required
        self.default = default
        if default:
            self.required = False
        self.type = type

    def process(self, name, value):
        if value is None:
            if self.required:
                raise ValueError(f"Missing parameter '{name}'.")
            elif self.default:
                return self.default
        else:
            if self.type and not isinstance(value, self.type):
                raise ValueError(f"Parameter '{name}' should be {self.type} but it is {type(value)}.")
        return value

    def extract(self, conf, action, attribute):
        value = get_single_param(conf, action, attribute)
        return self.process(attribute, value)

    def help_text(self):
        return 'a parameter' # TODO

class DBParameter(Parameter):
    def process(self, name, value):
        val = super().process(name, value)
        from ..db import RBBLFile
        return RBBLFile(val)

class QueryParameter(Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = dict
        self.required = True

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

class FeatureParameter(Parameter):
    def process(self, name, value):
        val = super().process(name, value)
        return parse_feature(val)

class UsernameParameter(Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.required = False
        self.type = str

    def process(self, name, value):
        val = super().process(name, value)
        if val is None:
            import os
            return os.environ.get('USER', 'script')
        return val
