#!/usr/bin/env python3

from rebabel_format.db import RBBLFile
from rebabel_format.config import parse_feature
from rebabel_format.query import search

from collections import defaultdict
import re

def transform(db, trans, username='user', confidence=1):
    for match in search(db, trans['query']):
        for cmd in trans['commands']:
            if cmd['type'] == 'set_feature':
                db.set_feature(match[cmd['target']], cmd['tier'],
                               cmd['feature'], cmd['value'], user=username,
                               confidence=confidence)
