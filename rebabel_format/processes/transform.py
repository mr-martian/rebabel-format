#!/usr/bin/env python3

from rebabel_format.process import Process
from rebabel_format.parameters import Parameter, UsernameParameter

class Transform(Process):
    name = 'transform'
    sequence = Parameter(type=list, help='the transformations to apply')
    user = UsernameParameter(help='the username to assign to changes')
    confidence = Parameter(default=1, type=int, help='the confidence value to assign to changes')

    def run(self):
        from rebabel_format.transform import transform
        from rebabel_format.config import get_single_param
        for rule in self.sequence:
            transform(self.db, get_single_param(self.conf, 'transform', rule),
                      username=self.user, confidence=self.confidence)
