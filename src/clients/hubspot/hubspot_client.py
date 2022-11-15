import json
import time
from collections import ChainMap
from threading import current_thread, get_ident, get_native_id

from hubspot.auth.oauth.api.tokens_api import TokensApi
from hubspot.auth.oauth.exceptions import ApiException
from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict

from src.clients.aws.aws_tools import Secret, search_secrets_by_prefix
from src.commons.client import Client
from src.utils.endpoint_utils import Endpoint
from src.utils.http_utils import get_http_adapter
from src.utils.various_utils import get_chunks, run_in_threads_pool


def contacts_pagination(endpoint, task_result):
    has_more_for_contacts = task_result.get("has-more", None)
    if not has_more_for_contacts:
        return None
    pagination_param = endpoint.get_param_by_name("vidOffset")
    pagination_param.value = task_result["vid-offset"]
    return endpoint


def companies_pagination(endpoint, task_result):
    has_more_for_companies = task_result.get("has-more", None)
    if not has_more_for_companies:
        return None
    pagination_param = endpoint.get_param_by_name("offset")
    pagination_param.value = task_result["offset"]
    return endpoint


def hasMore_offset_pagination(endpoint, task_result):
    has_more = task_result.get("hasMore", None)
    if not has_more:
        return None
    pagination_param = endpoint.get_param_by_name("offset")
    pagination_param.value = task_result["offset"]
    return endpoint


def contacts_recently_created_updated_pagination(endpoint, task_result):
    has_more = task_result.get("has-more", None)
    if not has_more:
        return None
    time_offset = task_result.get("time-offset", None)
    if time_offset:
        now = int(time.time())
        now_minus_one_hour_and_ten_minuts = now - 1000
        offset_limit_in_ms = now_minus_one_hour_and_ten_minuts * 1000
        if time_offset < offset_limit_in_ms:
            return None
        pagination_param = endpoint.get_param_by_name("timeOffset")
        pagination_param.value = time_offset
        return endpoint


class HubspotEndpoint(Endpoint):
    def __init__(
        self,
        params=None,
        url_template=None,
        query_params=None,
        portal_access_token_dict=None,
    ):
        super().__init__(
            params=params, url_template=url_template, query_params=query_params
        )
        self.portal_access_token_dict = portal_access_token_dict
        self._headers = None

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
        headers[
            "Authorization"
        ] = f"Bearer {self.portal_access_token_dict[self.params['portal_id']]}"

        return headers

    @property
    def headers(self):
        if self._headers is None:
            headers = self.build_headers()
            self._headers = headers
        return self._headers


class HubspotClient(Client):
    def __init__(self, task=None, env=None, db_connection=None):
        super().__init__(env)
        self.task = task
        self.http_adapter = get_http_adapter()
        self.db_connection = db_connection

    def get_access_token(self, portal_id):
        pass

    def get(self, task_params, **_ignored):

        # Construire les db_params
        db_params = self.get_request_params()

        # Get list of secrets by portal_id.
        secrets_list = search_secrets_by_prefix("hubspot/api/")
        portal_token_dict = {}
        for secret in secrets_list.get("SecretList", []):
            tmp_secret_value = Secret(secret["Name"]).get_value()
            refresh_token = tmp_secret_value["refresh_token"]
            refresh_token_params = {
                "grant_type": "refresh_token",
                "client_id": "fa6040db-72c2-4cca-b529-34d951716062",
                "client_secret": "f5d8dedb-7d18-4c19-a2a7-d25053d9c6cb",
                "redirect_uri": "https://connect.jabmo.app",
                # "refresh_token": "eu1-e86f-fc6e-4e94-8ff2-43870c0d1b4f",
                "refresh_token": refresh_token,
            }

            # Get access token
            try:
                access_token = (
                    TokensApi().create_token(**refresh_token_params).access_token
                )  # noqa: E501
            except ApiException as e:
                message = json.loads(e.body)["message"]
                print(f"{message} for portal_id {tmp_secret_value['portal_id']}")
                # TODO: Launch sentry alert
                access_token = None
            except Exception as e:
                print(e)
                raise (f"Unhandeld exception occurs: {e}")

            if access_token:
                portal_token_dict[str(tmp_secret_value["portal_id"])] = access_token

        request_params = [
            {
                k: {
                    "endpoint": HubspotEndpoint(
                        params=v,
                        url_template=task_params["query"]["template"],
                        # query_params=task_params["query"]["params"],
                        query_params=task_params["query"],
                        portal_access_token_dict=portal_token_dict,
                    )
                }
            }
            for k, v in db_params.items()
        ]
        result = []

        if self.task.name in [
            "contacts",
            "contacts_recently_created",
            "contacts_recently_updated",
            "companies",
            "companies_recently_updated",
            "campaigns",
            "campaign_details",
            "company_contact_associations",
            "contact_company_associations",
            "contact_created_company_daily_associations",
            "contact_updated_company_hourly_associations",
            "email_events_click_since_2022",
            "email_events_open_since_2022",
            "email_events_forward_since_2022",
            "email_events_click_daily",
            "email_events_open_daily",
            "email_events_forward_daily",
        ]:
            # db_params = self.get_request_params()

            print(f"Number of requests to run: {len(request_params)}")

            futures_results = []

            endpoint_list_list = get_chunks(request_params, chunk_size=100)
            for lst in endpoint_list_list:
                # for lst in [endpoint_list_list[0]]:

                print(f"Chunck with {len(lst)} queries")

                pagination_function = None
                if self.task.name in [
                    "contacts_recently_created",
                    "contacts_recently_updated",
                ]:
                    pagination_function = contacts_recently_created_updated_pagination

                if self.task.name == "contacts":
                    pagination_function = contacts_pagination

                if self.task.name == "companies":
                    pagination_function = companies_pagination

                if self.task.name in [
                    "campaigns",
                    "companies_recently_updated",
                    "contact_company_associations",
                    "contact_updated_company_hourly_associations",
                    "contact_created_company_daily_associations",
                    "email_events_open_daily",
                    "email_events_click_daily",
                    "email_events_forward_daily",
                    "email_events_click_since_2022",
                    "email_events_open_since_2022",
                    "email_events_forward_since_2022",
                ]:
                    pagination_function = hasMore_offset_pagination
                chunks_result_list = run_in_threads_pool(
                    request_params_list=lst,
                    source_function=self.do_get_query,
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

        return result

    def do_get_query(self, endpoint=None):
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
                f"{cpt} attemp(s) failed. Restarting thread after 100 seconds of pause: name={c_thread.name}, idnet={get_ident()}, id={get_native_id()}"  # noqa: E501
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
                # print(f"Endpoint: {endpoint}")
                # print(f"{response.reason} - {response.text}")
                raise ("Error while processing request")

        response = response.json()
        if "ServiceErrorCode" in response:
            print(f'ServiceErrorCode: {response["ServiceErrorCode"]}')
            return None

        return (response, endpoint)
