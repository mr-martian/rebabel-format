#!/usr/bin/env python3

from .db import RBBLFile
from .config import parse_feature
from .query import search

from collections import defaultdict
import re

def transform(db, trans, username='user', confidence=1):
    for match in search(db, trans['query']):
        for cmd in trans['commands']:
            if cmd['type'] == 'set_feature':
                db.set_feature(match[cmd['target']], cmd['tier'],
                               cmd['feature'], cmd['value'], user=username,
                               confidence=confidence)

def transform_conf(conf):
    from .config import get_single_param, get_user
    if 'transform' not in conf:
        raise ValueError('Missing transform')
    db = RBBLFile(get_single_param(conf, 'transform', 'db'))
    seq = get_single_param(conf, 'transform', 'sequence')
    user = get_user(conf, 'transform')
    confidence = get_single_param(conf, 'transform', 'confidence')
    if confidence is None:
        confidence = 1
    for rule in seq:
        transform(db, get_single_param(conf, 'transform', rule),
                  username=user, confidence=confidence)
