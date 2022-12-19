import json
import time
from collections import ChainMap
from threading import current_thread, get_ident, get_native_id

from hubspot.auth.oauth.api.tokens_api import TokensApi
from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict

from src.clients.aws.aws_tools import Secret, search_secrets_by_prefix
from src.commons.client import Client
from src.utils.endpoint_utils import Endpoint
from src.utils.http_utils import get_http_adapter
from src.utils.various_utils import get_chunks, run_in_threads_pool

refresh_token_params = {
    "grant_type": "refresh_token",
    "client_id": "fa6040db-72c2-4cca-b529-34d951716062",
    "client_secret": "f5d8dedb-7d18-4c19-a2a7-d25053d9c6cb",
    "redirect_uri": "https://connect.jabmo.app",
    "refresh_token": "9c2cb2f5-b460-4bea-b1a1-b1df862bc3c1",
}


# def hasMore_offset_pagination(endpoint, task_result):
#     has_more = task_result.get("hasMore", None)
#     if not has_more:
#         return None
#     pagination_param = endpoint.get_param_by_name("offset")
#     pagination_param.value = task_result["offset"]
#     return endpoint


def pageOffset_pagination(endpoint, task_result):
    pagecount = task_result.get("pagecount", None)
    currentpage = task_result.get("currentpage", None)
    if currentpage == pagecount - 1:
        return None
    pagination_param = endpoint.get_param_by_name("pageOffset")
    pagination_param.value = currentpage + 1
    return endpoint


def all_events(endpoint, task_result):
    # Need to use special pagination here because this endpoint doesn't allow more than 180 days for startDate.
    # If there is a gap of more than 180 days between 2 events,
    # pagination will stop and older events will not be retrieved.
    totalevents = task_result.get("totalevents", None)
    if totalevents == 0:
        return None
    pagination_param = endpoint.get_param_by_name("dateIntervalOffset")
    current_value = pagination_param.formatted_value
    pagination_param.value = current_value + 180
    return endpoint


class On24Endpoint(Endpoint):
    def __init__(
        self,
        params=None,
        url_template=None,
        query_params=None,
        access_token_key=None,
        access_token_secret=None
    ):
        super().__init__(
            params=params, url_template=url_template, query_params=query_params
        )
        self.access_token_key = access_token_key
        self.access_token_secret = access_token_secret
        self._headers = None

    def get_access_token(self):
        access_token = (
            TokensApi()
            .create_token(**refresh_token_params)
            .access_token  # noqa: E501
        )
        return access_token

    def build_headers(self):
        """# noqa: E501
        Build the header of the http request

        Use the access_token and optionnal header defined in json.

        Args:
            header: dict

        Returns:
            A dict of the build header.
        """
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["cache-control"] = "no-cache"
        headers["accessTokenKey"] = self.access_token_key
        headers["accessTokenSecret"] = self.access_token_secret
        return headers

    @property
    def headers(self):
        if self._headers is None:
            headers = self.build_headers()
            self._headers = headers
        return self._headers


class On24Client(Client):
    def __init__(self, task=None, env=None, db_connection=None):
        super().__init__(env)
        self.task = task
        self.http_adapter = get_http_adapter()
        self.db_connection = db_connection

    def get_access_token(self, portal_id):
        pass

    def get(self, task_params, **_ignored):

        secrets_list = search_secrets_by_prefix("on24/api/")
        portal_token_list = []
        for secret in secrets_list.get("SecretList", []):
            tmp_secret_value = Secret(secret["Name"]).get_value()
            portal_token_list.append(
                {
                    "accessTokenKey": tmp_secret_value["accessTokenKey"],
                    "accessTokenSecret": tmp_secret_value["accessTokenSecret"],
                }
            )

        # for ptd in [portal_token_list[2]]:
        for ptd in portal_token_list:
            # header = dict(**ptd)
            # header["Accept"] = "application/json"
            # header["cache-control"] = "no-cache"
            result = []

            db_params = self.get_request_params()
            total_requests_number = len(db_params)
            # endpoint_list = [
            #     (
            #         Endpoint(
            #             params=v,
            #             url_template=task_params["query"]["template"],
            #             query_params=task_params["query"]["params"],
            #         ),
            #         k,
            #     )
            #     for k, v in db_params.items()
            # ]
            print(f"Number of requests to run: {total_requests_number}")

            request_params = [
                {
                    k: {
                        "endpoint": On24Endpoint(
                            params=v,
                            url_template=task_params["query"]["template"],
                            query_params=task_params["query"],
                            access_token_key=ptd["accessTokenKey"],
                            access_token_secret=ptd["accessTokenSecret"]
                        )
                    }
                }
                for k, v in db_params.items()
            ]
            result = []

            futures_results = []

            endpoint_list_list = get_chunks(request_params, chunk_size=100)
            for lst in endpoint_list_list:
                # for lst in [endpoint_list_list[0]]:
                #headers = header

                # for ll in lst:
                #     ll[0].access_token = access_token

                print(f"Chunck with {len(lst)} queries")

                pagination_function = None
                if self.task.name in [
                    "all_registrants",
                    "all_attendees",
                    "events_updated",
                    "events_modified",
                ]:
                    pagination_function = pageOffset_pagination

                if self.task.name == "all_events":
                    pagination_function = all_events

                chunks_result_list = run_in_threads_pool(
                    request_params_list=lst,
                    source_function=self.do_get_query,
                    # headers=headers,
                    result_key=task_params["query"]["response_datas_key"],
                    pagination_function=pagination_function
                    if pagination_function
                    else None,
                )
                futures_results.extend(chunks_result_list)

            for fr in futures_results:
                local_result = []
                for k, v in fr.items():
                    for r in v:
                        data_to_add_to_results = []
                        if task_params.get(
                            "fields_to_add_to_api_result", None
                        ):  # noqa: E501
                            data_to_add_to_results = [
                                {e["destination_key"]: db_params[k][e["origin_key"]]}
                                for e in task_params[
                                    "fields_to_add_to_api_result"
                                ]  # noqa: E501
                            ]
                        api_data = (
                            r
                            if isinstance(r, dict)
                            else {task_params["key_for_values"]: r}
                        )
                        local_result.append(
                            dict(
                                ChainMap(*data_to_add_to_results, api_data)
                            )  # noqa: E501
                        )
                result.extend(local_result)

        to_return = [{"datas": d} for d in result]
        return to_return

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
            response = self.http_adapter.get(
                url=endpoint.get_endpoint_as_string(), headers=endpoint.headers
            )
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

            print("429 limit reached. Wait 100 seconds.")
            time.sleep(300)
            c_thread = current_thread()
            print(
                f"{cpt} attempt(s) failed. Restarting thread after 100 seconds of pause: name={c_thread.name}, idnet={get_ident()}, id={get_native_id()}"  # noqa: E501
            )
            response = self.do_get_query(endpoint=endpoint, headers=headers)
            if cpt == 5:
                raise TimeoutError("Failed to reach endpoint: {endpoint}")

            return None

        if response.status_code != 200:
            # TODO: Manage differents error cases.
            if (
                response.status_code == 404
                and "Unknown email campaign id"
                in json.loads(response.text)["message"]  # noqa: E501
            ):
                return None
            elif response.status_code == 401:
                pass

            else:
                print(f"Endpoint: {endpoint.get_endpoint_as_string()}")
                print(f"{response.reason} - {response.text}")
                raise Exception("Error while processing request")

        response = response.json()
        if "ServiceErrorCode" in response:
            print(f'ServiceErrorCode: {response["ServiceErrorCode"]}')
            return None

        return (response, endpoint)

    def build_headers(self, header=None, access_token=None):
        """# noqa: E501
        Build the header of the http request

        Use the access_token and optionnal header defined in json.

        Args:
            header: dict

        Returns:
            A dict of the build header.
        """
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["cache-control"] = "no-cache"
        headers["Authorization"] = f"Bearer {access_token}"

        if header:
            headers.update(header)

        return headers
