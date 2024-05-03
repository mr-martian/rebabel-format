#!/usr/bin/env python3

from .db import RBBLFile
from .query import search

def get_label(db, labels, units):
    ret = []
    for l in labels:
        utype = db.get_unit_type(units[l['unit']])
        fid, vtype = db.get_feature(utype, l['tier'], l['feature'])
        ret.append(str(db.get_feature_value(units[l['unit']], fid, vtype)))
    return ' '.join(ret)

def get_child_bound(db, uid, child_type, bound, right):
    children = db.get_units(child_type, parent=uid)
    fid, ftype = db.get_feature(child_type, 'meta', 'index')
    idx = db.get_feature_values(children, fid, ftype)
    children.sort(key=lambda c: idx.get(c, bound))
    if right:
        for u in children:
            if idx.get(u, bound) > bound:
                return u
    else:
        for u in reversed(children):
            if idx.get(u, bound) < bound:
                return u

def get_edge(db, uid, child_type, right=True):
    import math
    bound = -math.inf if right else math.inf
    u = get_child_bound(db, uid, child_type, bound=bound, right=right)
    if not u:
        u = get_edge(db, get_next(db, uid, right), child_type, right)
    return u

def get_next(db, uid, right=True):
    utype = db.get_unit_type(uid)
    fid, ftype = db.get_feature(utype, 'meta', 'index')
    idx = db.get_feature_value(uid, fid, ftype)
    parent = db.get_parent(uid)
    if not parent:
        return
    ret = get_child_bound(db, parent, utype, idx, right)
    if not ret:
        ret = get_edge(db, get_next(db, parent, right), utype, right)
    return ret

def get_span(db, uid, width):
    left = []
    right = []
    l = uid
    r = uid
    for i in range(width):
        l = get_next(db, l, False)
        r = get_next(db, r, True)
        left.append(l)
        right.append(r)
    return list(reversed(left)) + [uid] + right

def print_span(db, span, show):
    utype = db.get_unit_type(span[0])
    fid, ftype = db.get_feature(utype, *show)
    print(' '.join(str(db.get_feature_value(u, fid, ftype)) for u in span))

def concordance_conf(conf):
    from .config import get_single_param, parse_feature
    db = RBBLFile(get_single_param(conf, 'concordance', 'db'))
    query = get_single_param(conf, 'concordance', 'query')
    center = get_single_param(conf, 'concordance', 'center')
    if center is None:
        center = 'Center'
    width = get_single_param(conf, 'concordance', 'width')
    if width is None:
        width = 2
    label = get_single_param(conf, 'concordance', 'label')
    show = parse_feature(get_single_param(conf, 'concordance', 'print'))
    for result in search(db, query):
        if label:
            print(get_label(db, label, result), end=': ')
        span = get_span(db, result[center], width)
        print_span(db, span, show)
