# def nested_get(dic, keys):
#     try:
#         for key in keys:
#             dic = dic[key]
#         return dic
#     except Exception as e:
#         return None


def nested_get(dic, keys):
    try:
        for key in keys:
            if isinstance(dic, list):
                dic = dic[0][key]
            else:
                dic = dic[key]
        return dic
    except Exception as e:
        return None


from suds.sudsobject import asdict


def recursive_asdict(d):
    """Convert Suds object into serializable format."""
    out = {}
    for k, v in asdict(d).items():
        if hasattr(v, "__keylist__"):
            out[k] = recursive_asdict(v)
        elif isinstance(v, list):
            out[k] = []
            for item in v:
                if hasattr(item, "__keylist__"):
                    out[k].append(recursive_asdict(item))
                else:
                    out[k].append(item)
        else:
            out[k] = "%s" % v
    return out
