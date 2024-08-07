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
    elif isinstance(obj, list) and len(obj) == 2:
        return obj[0], obj[1]
    else:
        raise ValueError(f'Invalid feature specifier {obj}.')

def get_user(conf, action):
    user = get_single_param(conf, action, 'username')
    if user is None:
        import os
        user = os.environ.get('USER', action+'-script')
    return user
