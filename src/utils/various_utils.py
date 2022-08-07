import datetime
import itertools as it
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
    request_params_list=None,
    source_function=None,
    max_workers=40,
    headers=None,
    result_key=None,
    pagination_function=None,
    check_has_more_result_function=None,
    build_get_more_function=None,
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

    def contacts_pagination(endpoint, task_result):
        has_more_for_contacts = task_result.get("has-more", None)
        if not has_more_for_contacts:
            return None
        pagination_param = endpoint.get_param_by_name("vidOffset")
        pagination_param.value = task_result["vid-offset"]
        return endpoint

    threads_list = []
    result = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for request_params in request_params_list:
            threads_list.append(
                (
                    executor.submit(
                        source_function,
                        endpoint=request_params[0],
                        headers=headers,
                    ),
                    request_params[1],
                )
            )
        for task in threads_list:

            if task[0].result():
                task_result, endpoint = task[0].result()
                has_more = task_result.get("hasMore", None)
                has_more_for_contacts = task_result.get("has-more", None)
                tmp_result = []
                if result_key:
                    tmp_result.extend(task_result[result_key])
                else:
                    tmp_result.append(task_result)

                endpoint = contacts_pagination(endpoint, task_result)
                while endpoint:
                    int_task = executor.submit(
                        source_function,
                        endpoint=endpoint,
                        headers=headers,
                    )
                    task_result, endpoint = int_task.result()
                    if task_result:
                        if task_result[result_key]:
                            tmp_result.extend(task_result[result_key])
                        endpoint = contacts_pagination(endpoint, task_result)

                # while has_more or has_more_for_contacts:
                #     if has_more_for_contacts:
                #         pagination_param = endpoint.get_param_by_name("timeOffset")
                #         pagination_param.value = task_result["time-offset"]
                #     if has_more:
                #         pagination_param = endpoint.get_param_by_name("offset")
                #         pagination_param.value = task_result["offset"]
                #     int_task = executor.submit(
                #         source_function,
                #         endpoint=endpoint,
                #         headers=headers,
                #     )
                #     # tmpr = int_task.result()
                #     # if tmpr:
                #     task_result, endpoint = int_task.result()
                #     if task_result:
                #         if task_result[result_key]:
                #             tmp_result.extend(task_result[result_key])
                #     has_more = task_result.get("hasMore", None)
                #     has_more_for_contacts = task_result.get("has-more", None)
                #     time_offset_for_contacts = task_result.get("time-offset", None)
                #     if time_offset_for_contacts:
                #         tod = datetime.datetime.now().replace(
                #             hour=0, minute=0, second=0, microsecond=0
                #         )
                #         d = datetime.timedelta(days=1)
                #         yesterday = tod - d
                #         if (
                #             time_offset_for_contacts / 1000
                #             < datetime.datetime.timestamp(yesterday)
                #         ):
                #             has_more_for_contacts = None

                result.append({task[1]: tmp_result})
                print(f"# requests run so far: {len(result)}")
    return result


def zip_longest_repeat_value(*iterables):
    iterators = [iter(i) for i in iterables]  # make sure we're operating on iterators
    heads = [
        next(i) for i in iterators
    ]  # requires each of the iterables to be non-empty
    sentinel = object()
    iterators = [
        it.chain((head,), iterator, (sentinel,), it.repeat(head))
        for iterator, head in zip(iterators, heads)
    ]
    # Create a dedicated iterator object that will be consumed each time a 'sentinel' object is found.
    # For the sentinel corresponding to the last iterator in 'iterators' this will leak a StopIteration.
    running = it.repeat(None, len(iterators) - 1)
    iterators = [
        map(
            lambda x, h: next(running) or h
            if x is sentinel
            else x,  # StopIteration causes the map to stop iterating
            iterator,
            it.repeat(head),
        )
        for iterator, head in zip(iterators, heads)
    ]
    return zip(*iterators)
