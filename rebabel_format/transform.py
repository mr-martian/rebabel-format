#!/usr/bin/env python3

from rebabel_format.parameters import Parameter, process_parameters
from rebabel_format.query import search

class Transformation:
    name = None
    parameters = {}

    ALL = {}

    def __init__(self, conf=None, **kwargs):
        self.parameter_values = process_parameters(self.parameters, conf or {},
                                                   None, kwargs)

    def __init_subclass__(cls, *args, **kwargs):
        super(Transformation, cls).__init_subclass__(*args, **kwargs)
        if cls.name:
            if cls.name in Transformation.ALL:
                raise ValueError(f'Name {cls.name} is already used by another Transfomration class.')
            Transformation.ALL[cls.name] = cls

    def apply(self, db, match_dict):
        pass

class CreateFeature(Transformation):
    name = 'create_feature'

    unit_type = Parameter(type=str)
    feature = Parameter(type=str)
    value_type = Parameter(type=str)

    def apply(self, db, match_dict):
        db.create_feature(self.unit_type, self.feature, self.value_type)

class SetFeature(Transformation):
    name = 'set_feature'

    target = Parameter(type=str)
    feature = Parameter(type=str)
    value = Parameter()
    username = Parameter(type=str)
    confidence = Parameter(type=int, default=1)

    def apply(self, db, match_dict):
        if self.target not in match_dict:
            raise ValueError(f'No such unit {self.target}.')
        db.set_feature(match_dict[self.target], self.feature, self.value,
                       user=self.username, confidence=self.confidence)

class SetRefFeature(SetFeature):
    name = 'set_ref_feature'

    value = Parameter(type=str)

    def apply(self, db, match_dict):
        if self.target not in match_dict:
            raise ValueError(f'No such unit {self.target}.')
        if self.value not in match_dict:
            raise ValueError(f'No such unit {self.value}.')
        db.set_feature(match_dict[self.target], self.feature, match_dict[self.value],
                       user=self.username, confidence=self.confidence)

class CreateUnit(Transformation):
    name = 'create_unit'

    unit_type = Parameter(type=str)
    unit_name = Parameter(type=str)
    username = Parameter(type=str)

    def apply(self, db, match_dict):
        match_dict[self.unit_name] = db.create_unit(self.unit_type,
                                                    user=self.username)

class EditRelation(Transformation):
    parent = Parameter(type=str)
    child = Parameter(type=str)

    adding = True
    primary = True

    def apply(self, db, match_dict):
        if self.parent not in match_dict:
            raise ValueError(f'No such unit {self.parent}.')
        if self.child not in match_dict:
            raise ValueError(f'No such unit {self.child}.')
        if self.adding:
            db.set_parent(match_dict[self.parent], match_dict[self.child],
                          primary=self.primary)
        else:
            db.rem_parent(match_dict[self.parent], match_dict[self.child],
                          primary_only=self.primary)

class SetParent(EditRelation):
    name = 'set_parent'
    adding = True
    primary = True
class RemParent(EditRelation):
    name = 'remove_parent'
    adding = False
    primary = True
class SetRelation(EditRelation):
    name = 'set_relation'
    adding = True
    primary = False
class RemRelation(EditRelation):
    name = 'remove_relation'
    adding = False
    primary = False

def apply_transformations(db, query, commands):
    for match_dict in search(db, query):
        for cmd in commands:
            cmd.apply(db, match_dict)

def transform(db, transformations, username='user', confidence=1):
    trans = []
    for i, cmd in enumerate(transformations['commands'], 1):
        if 'type' not in cmd:
            raise ValueError(f'Command {i} is missing type.')
        cls = Transformation.ALL.get(cmd['type'])
        if cls is None:
            raise ValueError(f'No command named {cmd["type"]}.')
        try:
            trans.append(cls(cmd, username=username, confidence=confidence))
        except:
            raise ValueError(f'Command {i} has invalid arguments.')
    apply_transformations(db, transformations['query'], trans)
