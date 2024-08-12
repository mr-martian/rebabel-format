#!/usr/bin/env python3

from .process import Process
from .parameters import Parameter, UsernameParameter

class Transform(Process):
    name = 'transform'
    sequence = Parameter(type=list, help='the transformations to apply')
    user = UsernameParameter(help='the username to assign to changes')
    confidence = Parameter(default=1, type=int, help='the confidence value to assign to changes')

    def run(self):
        from ..transform import transform
        from ..config import get_single_param
        for rule in self.sequence:
            transform(self.db, get_single_param(self.conf, 'transform', rule),
                      username=self.username, confidence=self.confidence)
