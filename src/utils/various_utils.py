import os

from suds.sudsobject import asdict

# def nested_get(dic, keys):
#     try:
#         for key in keys:
#             if isinstance(dic, list):
#                 dic = dic[0][key]
#             else:
#                 dic = dic[key]
#         return dic
#     except Exception:
#         return None


# def nested_get(dic, keys):
#     try:
#         for key in keys:
#             if not isinstance(key, int):
#                 if isinstance(dic, list):
#                     try:
#                         dic = dic[0][key]
#                     except:
#                         dic = dic[0]
#                 else:
#                     dic = dic[key]
#             else:
#                 dic = dic[key]
#         return dic
#     except Exception:
#         return None


def nested_get(dic, keys):

    try:
        if keys:
            for key in keys:
                dic = dic[key]
            return dic
        else:
            return None
    except KeyError:
        return None


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


def get_running_env():
    running_env = os.environ.get("RUNNING_ENV")
    if not running_env:
        raise ValueError("RUNNING_ENV cannot be None.")
    return running_env


def get_schema_name(channel):
    schema_name = channel + "_" + get_running_env()
    return schema_name
