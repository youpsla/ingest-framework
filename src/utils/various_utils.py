import os

from suds.sudsobject import asdict


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
    # return schema_name
    return "hubspot_development"


def get_chunks(source_list, chunk_size=500):
    """
    Split a list in n list of chunk_size elements

    Args:
        source_list: The list to be split. Default to 500.
        chunk_size: Max size of each chunk

    Returns:
        A list of list(s).

    """
    if len(source_list) > chunk_size:
        chunks_lists = [
            source_list[offs : offs + chunk_size]
            for offs in range(0, len(source_list), chunk_size)
        ]
        return chunks_lists
    else:
        return [source_list]


from concurrent import futures


def run_in_threads_pool(
    request_params_list=None, source_function=None, max_workers=40, headers=None
):
    """
    Runs a function in a thread pool.

    Args:
        request_params_list: List of list of args passed to source_function
        source_function: The function to run in each thread of the pool
        nb_workers: Number of threads int he pool. Default to 40.
        headers: In

    Returns:
        A list of results. Result is the result property of the Future.

    """
    threads_list = []
    result = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for request_params in request_params_list:
            threads_list.append(
                (
                    executor.submit(source_function, request_params[0], headers),
                    request_params[1],
                )
            )

            for task in threads_list:
                task_result = task[0].result()
                if task_result is not None:
                    result.append({task[1]: task_result})
    return result
