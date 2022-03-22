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
