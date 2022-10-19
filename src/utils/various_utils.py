import itertools as it
import json
import os
from concurrent import futures

from suds.sudsobject import asdict


def nested_get(dic, keys):

    try:
        if keys:
            for key in keys:
                if dic:
                    dic = dic[key]
                else:
                    return None
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
    # return "hubspot_development"


def get_model_params_as_dict(channel, model_name):
    app_home = os.environ.get("APPLICATION_HOME")
    model_file = os.path.join(app_home, "configs", channel, "models.json")
    with open(model_file, "r") as f:
        models_list = json.load(f)
        for model in models_list:
            if model_name in model:
                return model[model_name]


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
            source_list[offs : offs + chunk_size]  # noqa: E203
            for offs in range(0, len(source_list), chunk_size)
        ]
        return chunks_lists
    return [source_list]


def run_in_threads_pool(
    request_params_list=None,
    source_function=None,
    max_workers=40,
    headers=None,
    result_key=None,
    pagination_function=None,
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
                tmp_result = []
                # If there is a result_key in task prarams, we use it to retrieve only relevant data.
                # Otherwise, we use all data received.
                if result_key:
                    # Sometimes the value task_result[result_key] can be a list, sometimes a dict
                    # We test the type and adapt the way we add to tmp_result
                    #    - type list: We 'extend' tmp_result
                    #    - type dict: We 'extend' tmp_result with the values of the dict
                    l_result = task_result[result_key]
                    if type(l_result) is list:
                        tmp_result.extend(l_result)
                    if type(l_result) is dict:
                        tmp_result.extend(list(l_result.values()))
                else:
                    tmp_result.append(task_result)

                # If there is a pagination function for the task, we do requests until end of pagination has been reached.
                if pagination_function:
                    # We retrieve the endpoint with parameters updated depending on pagination result retrieved (start, count, ....)
                    endpoint = pagination_function(endpoint, task_result)
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
                            endpoint = pagination_function(
                                endpoint, task_result
                            )  # noqa: E501

                result.append({task[1]: tmp_result})
                # print(f"# requests run so far: {len(result)}")
    return result


def zip_longest_repeat_value(*iterables):
    """Equalize size of iterables to the longuest one. The latest value "short" irerable is repeated.
    Inputs:
        - [1,2,3]
        - [A,B]
        - [F]

    Output:
        [[1,2,3], [A,B,B], [F,F,F]]
    """
    iterators = [
        iter(i) for i in iterables
    ]  # make sure we're operating on iterators # noqa: E501
    heads = [
        next(i) for i in iterators
    ]  # requires each of the iterables to be non-empty
    sentinel = object()
    iterators = [
        it.chain((head,), iterator, (sentinel,), it.repeat(head))
        for iterator, head in zip(iterators, heads)
    ]
    # Create a dedicated iterator object that will be consumed each time a 'sentinel' object is found. # noqa: E501
    # For the sentinel corresponding to the last iterator in 'iterators' this will leak a StopIteration. # noqa: E501
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
