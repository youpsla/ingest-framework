import time
from threading import current_thread, get_ident, get_native_id
from urllib.parse import urlencode

from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from src.commons.client import Client
from src.utils.http_utils import get_http_adapter

from hubspot import HubSpot


class HubspotClient(Client):
    def __init__(self, task=None, env=None):
        super().__init__(env)
        self.task = task
        self.api_client = HubSpot(api_key="eu1-fd74-6c90-4dc8-a93b-a1b33969e03c")
        self.http_adapter = get_http_adapter()

    def get_endpoints_list(self):
        try:
            params = self.task.params["url"]["params"]
        except KeyError:
            params = []
        request_params_list = self.get_request_params(self.task)
        dynamics_params = self.get_dynamics_params(params)
        statics_params = self.get_statics_params(params)
        kwargs = dynamics_params + statics_params
        kwargs = self.get_kwargs_list()
        endpoints_list = []
        if not request_params_list:
            return [(self.build_endpoint(base=self.task.params["url"]["base"]), [])]
        for rp in request_params_list:
            endpoints_list = [
                (
                    self.build_endpoint(
                        base=rp["base"],
                        kwargs=zd[1] + kwargs,
                        args=zd[2],
                    ),
                    zd[0],
                )
                for zd in request_params_list
            ]
        return endpoints_list

    def get_chunks(self, lst):
        chunk_size = 500
        if len(lst) > chunk_size:
            results_lists = [
                lst[offs : offs + chunk_size] for offs in range(0, len(lst), chunk_size)
            ]
            return results_lists
        else:
            return [lst]

    def get(self, task_params, **_ignored):

        result = []
        if self.task.name == "contacts":
            # services.get_service("contacts")
            result = self.api_client.crm.contacts.get_all()

        if self.task.name == "companies":
            # services.get_service("companies")
            result = self.api_client.crm.companies.get_all()

        if self.task.name in ["campaign_details", "campaigns", "email_events"]:
            # endpoints_list = self.get_endpoints_list()
            url_params = task_params["url"]
            params = url_params.get("params", None)

            if params:
                db_params = self.get_request_params(self.task)
                total_requests_number = len(db_params)
                dynamics_params = self.get_dynamics_params(params)
                statics_params = self.get_statics_params(params)
                kwargs = dynamics_params + statics_params
                kwargs = []
                endpoint_list = [
                    (
                        self.build_endpoint(
                            base=url_params["base"],
                            kwargs=zd[1] + kwargs,
                            args=zd[2],
                        ),
                        zd[0],
                    )
                    for zd in db_params
                ]
            else:
                total_requests_number = 1
                endpoint_list = [
                    (
                        self.build_endpoint(
                            base=url_params["base"],
                        ),
                        [],
                    )
                ]
            print(f"Number of requests to run: {total_requests_number}")

            import concurrent.futures

            ### Method 1. not sure to rpeserver order for adding avalues later
            # with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            #     futures = [
            #         executor.submit(self.do_get_query, endpoint, headers)
            #         for endpoint in endpoint_list
            #     ]
            # futures_result = [f.result() for f in futures]

            futures_results = []
            response_key = url_params.get("response_datas_key", None)
            endpoint_list_list = self.get_chunks(endpoint_list)
            for lst in endpoint_list_list:
                threads = []
                print(f"Chunck with {len(lst)} queries")
                with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
                    for endpoint in lst:
                        threads.append(
                            (
                                executor.submit(self.do_get_query, endpoint[0]),
                                endpoint[1],
                            )
                        )

                    for task in threads:
                        tmp_task_result = task[0].result()
                        if tmp_task_result is not None:
                            if response_key:
                                response_elements = tmp_task_result[response_key]
                            else:
                                response_elements = [tmp_task_result]
                            if len(response_elements) >= 15000:
                                print(
                                    "LINKEDIN API error. Max elements of 15 000 per request"
                                    f" reached. Elments for enpoint {endpoint} will not be"
                                    " inserted in Db.\n"
                                )
                                raise ValueError(
                                    "Linbkedin APi limit reached. More than 15000 elements in answer. STOPPING !!"
                                )
                            if task[1]:
                                for r in response_elements:
                                    for f in task[1]:
                                        for k, v in f.items():
                                            r[k] = v

                            futures_results.append(response_elements)

            result = []
            for r in futures_results:
                result.extend(r)

            to_return = [{"datas": d} for d in result]
            return to_return

        if self.task.name == "events":
            db_params = self.get_request_params(self.task)[0:20]
            if db_params:
                for param in db_params:
                    tmp = self.api_client.events.events_api.get_page(
                        **param[1][0]
                    ).results

                    if tmp:
                        for t in tmp:
                            for i in param[0]:
                                for k, v in i.items():
                                    t[k] = v
                            result.append(t)
            else:
                pass

        tmp = []
        for r in result:
            tmp.append({"datas": r.to_dict()})

        return tmp

    def build_endpoint(self, base=None, kwargs=None, args=None):
        """# noqa: E501
        Build the enpoint url with all args, kwargs.

        Use the access_token and optionnal header defined in json.

        Args:
            base: str
                Base url as retrieved from json definition
            category: str
                The type of API call.
            q: str
                The type of the query. Can be "search" or "analytics" for linkedin

        Returns:
            A str representing an url
        """
        if kwargs:
            kwargs_tuple = [(k, v) for f in kwargs for k, v in f.items()]
        endpoint = (
            f"{base}"
            f"{str(args[0]) if args else ''}"
            f"{'?' if kwargs else ''}"
            f"{'&' + urlencode(kwargs_tuple) if kwargs else ''}"
        )
        return endpoint

    def do_get_query(
        self,
        endpoint="",
        headers={},
    ):
        """# noqa: E501
        Qurey the API endpoint

        Args:
            endpoint: str
                The endpoint
            headers: dict
                Headers of the http request

        Returns:
            A json str repeenting the API response
        """
        cpt = 1
        try:
            response = self.http_adapter.get(url=endpoint, headers=headers)
        except ConnectionError as e:
            print("Error while connecting to db")
            print(e)
            return None
        except ConnectTimeout as e:
            print("Request timeout")
            print(e)
            return None
        except RetryError as e:
            print("Too many retries. 429.")
            print(e)

            print("429 limit reached. Wait 5 minutes.")
            time.sleep(300)
            c_thread = current_thread()
            print(
                f"{cpt} attemp(s) failed. Restarting thread after 300 seconds of pause: name={c_thread.name}, idnet={get_ident()}, id={get_native_id()}"
            )
            response = self.do_get_query(endpoint=endpoint, headers=headers)
            if cpt == 5:
                raise TimeoutError("Failed to reach endpoint: {endpoint}")

            return None
        except Exception as e:
            print("Unhandled exception occurs")
            print(e)
            raise ("Error while processing request")

        if response.status_code != 200:
            print("Error while processing request")
            print(f"Endpoint: {endpoint}")
            print(f"{response.reason} - {response.text}")
            # TODO: Manage differents error cases.
            raise ("Error while processing request")

        response = response.json()
        if "ServiceErrorCode" in response:
            print(f'ServiceErrorCode: {response["ServiceErrorCode"]}')
            return None

        return response
