import datetime
import json
import time
from collections import ChainMap
from threading import current_thread, get_ident, get_native_id
from urllib.parse import urlencode

from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict
from src.clients.aws.aws_tools import Secret, search_secrets_by_prefix
from src.commons.client import Client
from src.utils.endpoint_utils import Endpoint
from src.utils.http_utils import get_http_adapter
from src.utils.various_utils import get_chunks, run_in_threads_pool

from hubspot import HubSpot
from hubspot.auth.oauth.api.tokens_api import TokensApi

refresh_token_params = {
    "grant_type": "refresh_token",
    "client_id": "fa6040db-72c2-4cca-b529-34d951716062",
    "client_secret": "f5d8dedb-7d18-4c19-a2a7-d25053d9c6cb",
    "redirect_uri": "https://connect.jabmo.app",
    "refresh_token": "9c2cb2f5-b460-4bea-b1a1-b1df862bc3c1",
}


def contacts_pagination(endpoint, task_result):
    has_more_for_contacts = task_result.get("has-more", None)
    if not has_more_for_contacts:
        return None
    pagination_param = endpoint.get_param_by_name("vidOffset")
    pagination_param.value = task_result["vid-offset"]
    return endpoint


def companies_pagination(endpoint, task_result):
    has_more_for_contacts = task_result.get("has-more", None)
    if not has_more_for_contacts:
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
        tod = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        d = datetime.timedelta(days=1)
        yesterday = tod - d
        if time_offset / 1000 < datetime.datetime.timestamp(yesterday):
            return None
        pagination_param = endpoint.get_param_by_name("timeOffset")
        pagination_param.value = time_offset
        return endpoint


class HubspotClient(Client):
    def __init__(self, task=None, env=None, db_connection=None):
        super().__init__(env)
        self.task = task
        self.http_adapter = get_http_adapter()
        self.db_connection = db_connection

    def get_access_token(self, portal_id):
        pass

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

    def get(self, task_params, **_ignored):

        final_result = []
        secrets_list = search_secrets_by_prefix("hubspot/api/")
        portal_token_list = []
        for secret in secrets_list.get("SecretList", []):
            tmp_secret_value = Secret(secret["Name"]).get_value()
            portal_token_list.append(
                (
                    tmp_secret_value["portal_id"],
                    tmp_secret_value["access_token"],
                    tmp_secret_value["refresh_token"],
                )
            )

        for ptd in [portal_token_list[2]]:
            # for ptd in portal_token_list:
            access_token = ptd[1]
            portal_id = ptd[0]
            access_token = TokensApi().create_token(**refresh_token_params).access_token
            oauth_api_client = HubSpot(access_token=access_token)
            result = []

            tmp = None

            if self.task.name in ["events"]:
                db_params = self.get_request_params()
                cpt = 0
                if db_params:
                    for param in db_params:
                        try:
                            if self.task.name == "events":
                                _tmp_result = (
                                    oauth_api_client.events.events_api.get_page(
                                        **param[1][0],
                                        **param[1][1],
                                        limit=10000000,
                                    )
                                )
                            if self.task.name == "email_events":
                                _tmp_result = (
                                    oauth_api_client.crm.objects.emails.basic_api.get_page()
                                )
                        except:
                            refresh_token_params["refresh_token"] = ptd[2]
                            access_token = (
                                TokensApi()
                                .create_token(**refresh_token_params)
                                .access_token
                            )
                            oauth_api_client = HubSpot(access_token=access_token)
                            if self.task.name == "events":
                                _tmp_result = (
                                    oauth_api_client.events.events_api.get_page(
                                        **param[1][0],
                                        **param[1][1],
                                        limit=10000000,
                                    )
                                )
                            if self.task.name == "email_events":
                                _tmp_result = oauth_api_client.crm.objects.emails.basic_api.get_page(
                                    **param[1][0],
                                    **param[1][1],
                                    limit=10000000,
                                )

                        r = _tmp_result.results

                        cpt += 1
                        print(cpt)
                        result.extend(r)
                        print(len(result))
                else:
                    pass

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
                "contact_updated_company_daily_associations",
                "email_events_click_since_2022",
                "email_events_open_since_2022",
                "email_events_forward_since_2022",
                "email_events_click_daily",
                "email_events_open_daily",
                "email_events_forward_daily",
            ]:

                db_params = self.get_request_params()
                total_requests_number = len(db_params)
                endpoint_list = [
                    (
                        Endpoint(
                            params=v,
                            url_template=task_params["query"]["template"],
                            query_params=task_params["query"]["params"],
                        ),
                        k,
                    )
                    for k, v in db_params.items()
                ]
                print(f"Number of requests to run: {total_requests_number}")

                futures_results = []

                endpoint_list_list = get_chunks(endpoint_list, chunk_size=100)
                for lst in endpoint_list_list:
                    # for lst in [endpoint_list_list[0]]:
                    access_token = (
                        TokensApi().create_token(**refresh_token_params).access_token
                    )
                    headers = self.build_headers(header=None, access_token=access_token)

                    for l in lst:
                        l[0].access_token = access_token

                    print(f"Chunck with {len(lst)} queries")

                    pagination_function = None
                    if self.task.name in [
                        "contacts_recently_created",
                        "contacts_recently_updated",
                    ]:
                        pagination_function = (
                            contacts_recently_created_updated_pagination
                        )

                    if self.task.name == "contacts":
                        pagination_function = contacts_pagination

                    if self.task.name == "companies":
                        pagination_function = companies_pagination

                    if self.task.name in [
                        "companies_recently_updated",
                        "contact_company_associations",
                        "contact_updated_company_daily_associations",
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
                        headers=headers,
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
                            if task_params.get("fields_to_add_to_api_result", None):
                                data_to_add_to_results = [
                                    {
                                        e["destination_key"]: db_params[k][
                                            e["origin_key"]
                                        ]
                                    }
                                    for e in task_params["fields_to_add_to_api_result"]
                                ]
                            api_data = (
                                r
                                if isinstance(r, dict)
                                else {task_params["key_for_values"]: r}
                            )
                            local_result.append(
                                dict(ChainMap(*data_to_add_to_results, api_data))
                            )
                    result.extend(local_result)

            if result:
                for r in result:
                    r["portal_id"] = portal_id
                    final_result.append(r)
            else:
                pass

        to_return = [{"datas": d} for d in final_result]
        return to_return

        # if self.task.name == "events":
        #     db_params = self.get_request_params(self.task)
        #     if db_params:
        #         for param in db_params:
        #             tmp = self.oauth_api_client.events.events_api.get_page(
        #                 **param[1][0], **param[1][1]
        #             ).results

        #             if tmp:
        #                 for t in tmp:
        #                     for i in param[0]:
        #                         for k, v in i.items():
        #                             t[k] = v
        #                     result.append(t)
        #     else:
        #         pass

        tmp = []
        for r in result:
            tmp.append({"datas": r})

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
            f"/HUBSPOT_DEFINED/2"
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
            response = self.http_adapter.get(
                url=endpoint.get_endpoint_as_string(), headers=headers
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
                f"{cpt} attemp(s) failed. Restarting thread after 100 seconds of pause: name={c_thread.name}, idnet={get_ident()}, id={get_native_id()}"
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
                and "Unknown email campaign id" in json.loads(response.text)["message"]
            ):
                return None
            elif response.status_code == 401:
                pass

            else:
                print(f"Endpoint: {endpoint}")
                print(f"{response.reason} - {response.text}")
                raise ("Error while processing request")

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
