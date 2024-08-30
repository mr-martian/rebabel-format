#!/usr/bin/env python3

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

def read_config(pth):
    with open(pth, 'rb') as fin:
        return tomllib.load(fin)

def get_param(conf, *locs):
    for l in locs:
        if isinstance(l, str):
            if l in conf:
                return conf[l]
        elif isinstance(l, list):
            tmp = conf
            for k in l:
                if isinstance(tmp, dict) and k in tmp:
                    tmp = tmp[k]
                else:
                    break
            else:
                return tmp
    return None

def get_single_param(conf, action, key):
    return get_param(conf, [action, key], key)

def parse_feature(obj):
    if isinstance(obj, str) and obj.count(':') == 1:
        return tuple(obj.split(':'))
    elif isinstance(obj, dict) and 'tier' in obj and 'feature' in obj:
        return obj['tier'], obj['feature']
    elif isinstance(obj, dict) and 'feature' in obj:
        return parse_feature(obj['feature'])
    elif (isinstance(obj, list) or isinstance(obj, tuple)) and len(obj) == 2:
        return obj[0], obj[1]
    else:
        raise ValueError(f'Invalid feature specifier {obj}.')

def get_user(conf, action):
    user = get_single_param(conf, action, 'username')
    if user is None:
        import os
        user = os.environ.get('USER', action+'-script')
    return user

def parse_mappings(mappings):
    if not mappings:
        return {}, {}
    features = []
    type_dict = {}
    rev_type_dict = {}
    for i, mp in enumerate(mappings, 1):
        infeat = mp.get('in_feature')
        outfeat = mp.get('out_feature')
        intier = mp.get('in_tier')
        outtier = mp.get('out_tier')
        intype = mp.get('in_type')
        outtype = mp.get('out_type')
        if infeat and outfeat:
            if intier:
                f_in = (intier, infeat)
            else:
                f_in = parse_feature(infeat)
            if outtier:
                f_out = (outtier, outfeat)
            else:
                f_out = parse_feature(outfeat)
            features.append((f_in, intype, f_out, outtype))
        elif intype and outtype:
            type_dict[intype] = outtype
            rev_type_dict[outtype] = intype
        else:
            raise ValueError(f'Unable to interpret mapping {i}: {mp}.')
    feat_dict = {}
    for fi, ti, fo, to in features:
        if ti and not to:
            to = type_dict.get(ti, ti)
        elif to and not ti:
            ti = type_dict.get(to, to)
        feat_dict[(fi, ti)] = (fo, to)
    return type_dict, feat_dict
