def as_list(v):
    if isinstance(v, list):
        return v
    else:
        return [v]

def map_type(type_map, oldtype):
    if not type_map:
        return oldtype
    elif isinstance(oldtype, list):
        return [type_map.get(t, t) for t in old_type]
    else:
        return type_map.get(oldtype, oldtype)

def map_feature(feat_map, oldtype, oldfeat):
    if not feat_map:
        return oldfeat
    for typ in as_list(oldtype) + [None]:
        if (oldfeat, typ) in feat_map:
            return feat_map[(oldfeat, typ)][0]
        return oldfeat
