#!/usr/bin/env python3

from ..db import RBBLFile

ALL_WRITERS = {}

class BaseWriter:
    def __init__(self, db, conf):
        self.db = db
        self.conf = conf
        self.type_mapping = {} # src > dest
        self.rev_type_mapping = {} # dest > src
        self.feat_mapping = {}
        self.rev_feat_mapping = {}
        for k, v in conf.get('mapping', {}).items():
            to_type = k
            if 'type' in v:
                to_type = v['type']
                self.type_mapping[v['type']] = k
                self.rev_type_mapping[k] = v['type']
            for tier, fd in v.items():
                if not isinstance(fd, dict):
                    continue
                for featname, spec in fd.items():
                    to_tier = None
                    to_feat = None
                    if isinstance(spec, dict):
                        to_tier = spec.get('tier')
                        to_feat = spec.get('feature')
                    elif isinstance(spec, str) and spec.count(':') == 1:
                        to_tier, to_feat = spec.split(':')
                    if to_tier is None or to_feat is None:
                        raise ValueError('Invalid feature specifier %s.' % spec)
                    i, _ = self.db.get_feature(k, tier, featname, error=True)
                    self.feat_mapping[(to_type, to_tier, to_feat)] = i
                    self.rev_feat_mapping[i] = (to_type, to_tier, to_feat)

    def all_feats_from_tiers(self, tiers):
        ret = {}
        keep = [v for k,v in self.feat_mapping.items() if k[1] in tiers]
        for fid, tier, feat, utyp, vtyp in self.db.get_all_features():
            if fid in self.rev_feat_mapping:
                utyp, tier, feat = self.rev_feat_mapping[fid]
            if tier in tiers:
                ret[fid] = (tier, feat, vtyp)
        return ret

    def iter_units(self, utyp, featlist, parent=None):
        ls = self.db.get_units(self.type_mapping.get(utyp, utyp), parent=parent)
        ls.sort()
        for i in ls:
            yield i, self.db.get_unit_features(i, featlist)

    def write(self, fout):
        pass

class MetaWriter(type):
    def __new__(cls, name, bases, attrs):
        global ALL_WRITERS
        new_attrs = attrs.copy()
        ident = attrs.get('identifier')
        if ident in ALL_WRITERS:
            raise ValueError(f'Identifier {ident} is already used by another writer class.')
        for a, v in attrs.items():
            if a in ['identifier']:
                del new_attrs[a]
        ret = super(MetaWriter, cls).__new__(cls, name, bases, new_attrs)
        if ident is not None:
            ALL_WRITERS[ident] = ret
        return ret

class Writer(BaseWriter, metaclass=MetaWriter):
    pass

def write(conf):
    from ..config import get_single_param
    mode = get_single_param(conf, 'export', 'mode')
    if mode not in ALL_WRITERS:
        raise ValueError(f'Unknown writer {mode}.')
    in_path = get_single_param(conf, 'export', 'db')
    out_path = get_single_param(conf, 'export', 'outfile')
    db = RBBLFile(in_path)
    w = ALL_WRITERS[mode](db, conf)
    with open(out_path, 'w') as fout:
        w.write(fout)
