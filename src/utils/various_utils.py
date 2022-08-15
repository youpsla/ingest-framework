import itertools as it
import json
import os


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
    # Need to sort the 5 layers issue. Make task.py really independant from clients.
    pass


def get_running_env():
    running_env = os.environ.get("RUNNING_ENV")
    if not running_env:
        raise ValueError("RUNNING_ENV cannot be None.")
    return running_env


def get_schema_name(channel):
    schema_name = channel + "_" + get_running_env()
    # return schema_name
    return "hubspot_development"


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
                if result_key:
                    tmp_result.extend(task_result[result_key])
                else:
                    tmp_result.append(task_result)

                if pagination_function:
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
                            endpoint = pagination_function(endpoint, task_result)

                result.append({task[1]: tmp_result})
                # print(f"# requests run so far: {len(result)}")
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
