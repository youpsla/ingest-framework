import concurrent.futures
import json
import time
from threading import current_thread, get_ident, get_native_id
from urllib.parse import urlencode

from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict
from src.clients.aws.aws_tools import Secret, search_secrets_by_prefix
from src.commons.client import Client
from src.utils.http_utils import get_http_adapter

from hubspot import HubSpot
from hubspot.crm.companies.models.simple_public_object_with_associations import (
    SimplePublicObjectWithAssociations as companies_public_object,
)
from hubspot.crm.contacts.models.simple_public_object_with_associations import (
    SimplePublicObjectWithAssociations as contacts_public_object,
)


class HubspotClient(Client):
    def __init__(self, task=None, env=None, db_connection=None):
        super().__init__(env)
        self.task = task
        self.http_adapter = get_http_adapter()
        self.db_connection = db_connection
        # self.access_token = "CPH0rK6aMBIMggMAUAAAACAAAABIGI_HrQwghercFSj5iTgyFJ3Dsaw0d1ZAh1CzjMyZa9GbRFoVOjAAMWBB_wcAADwAtABg4HzOKIYAAGAAACA8ACAYAAAAwMN_NgEAAACBZxwY4AAAIAJCFPzpTnzjZne5_6-ZY8t8bz02VleySgNldTFSAFoA"

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

        final_result = []
        # if self.task.name == "contacts":
        #     # services.get_service("contacts")
        #     result = self.oauth_api_client.crm.contacts.get_all()

        # raw_sql = "select portal_id from hubspot_development.accounts"
        # accounts_list = SqlQuery(
        #     self.db_connection, qtype="raw_sql", raw_sql=raw_sql
        # ).run()

        secrets_list = search_secrets_by_prefix("hubspot/api/")
        portal_token_list = []
        for secret in secrets_list.get("SecretList", []):
            tmp_secret_value = Secret(secret["Name"]).get_value()
            portal_token_list.append(
                (tmp_secret_value["portal_id"], tmp_secret_value["access_token"])
            )

        # ptl = [(25912207, 'CLWs0tybMBIMggMAUAAAACAAAABIGI_HrQwghercFSj5iTgyFAncLX84DUTpinSCszbkBvScEXT4OjAAMWBB_wcAADwAtABg4HzOKIYAAGAAACA8ACAYAAAAwMN_NgEAAACBZxwY4AAAIAJCFMz1adgaAs5ngxV6JC62tGxh1YwMSgNldTFSAFoA'), ('25955882', 'CKi709ybMBIMggMAUAAAACAAAABIGKqcsAwgioLPDSj5iTgyFL8KPuPP69akxB2E6Rb-7K2WVw5TOjAAMWBB_wcAADwAtABg4HzOKIYAAGAAACA8ACAYAAAAwMN_NgEAAACBZxwY4AAAIAJCFFqrVj3APiWCbyvmw05zSi_EhpQySgNldTFSAFoA'), (1838475, 'CIab1dybMBIMggMAUAAAACAAAABIGIubcCDywqgNKPmJODIUWaoWClovooFugJn6lz4EhyJzb986MAAxYEH_BwAAHAC0AGDgfMYohgAAIAAAABwAIBgAAADAw382AQAAAIFnHBjAAAAgAkIU-PYny8M0Zbo9XURWCQr5O9lJ3MlKA25hMVIAWgA')]
        # portal_token_list = ptl[0:2]
        for ptd in portal_token_list:
            access_token = ptd[1]
            portal_id = ptd[0]
            oauth_api_client = HubSpot(access_token=access_token)
            result = []

            tmp = None

            if self.task.name == "contacts":
                tmp = oauth_api_client.crm.contacts.get_all()

            if self.task.name == "companies":
                tmp = oauth_api_client.crm.companies.get_all(
                    properties=[
                        "hs_analytics_num_page_views",
                        "hs_additional_domains",
                        "name",
                        "industry",
                    ]
                )

            if self.task.name == "events":
                tmp = []
                db_params = self.get_request_params(self.task)
                if db_params:
                    for param in db_params:

                        tmp = oauth_api_client.events.events_api.get_page()
                        _tmp_result = oauth_api_client.events.events_api.get_page(
                            **param[1][0], **param[1][1]
                        ).results

                        result.append(_tmp_result)
                        # tmp.append(_tmp_result)

                        # if tmp:
                        #     for t in tmp:
                        #         for i in param[0]:
                        #             for k, v in i.items():
                        #                 t[k] = v
                        #         result.append(t)
                else:
                    pass

            # if tmp:
            #     for t in tmp:
            #         if isinstance(t, (companies_public_object, contacts_public_object)):
            #             t = t.to_dict()
            #         t["portal_id"] = portal_id
            #         result.append(t)
            # else:
            #     pass

            if self.task.name in ["campaign_details", "campaigns", "email_events"]:

                headers = self.build_headers(header=None, access_token=access_token)
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
                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=40
                    ) as executor:
                        for endpoint in lst:
                            threads.append(
                                (
                                    executor.submit(
                                        self.do_get_query, endpoint[0], headers
                                    ),
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
                                if task[1]:
                                    for r in response_elements:
                                        for f in task[1]:
                                            for k, v in f.items():
                                                r[k] = v

                                futures_results.append(response_elements)

                for r in futures_results:
                    if r:
                        result.extend(r)

            if result:
                for r in result:
                    if isinstance(r, (companies_public_object, contacts_public_object)):
                        r = r.to_dict()
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
            # TODO: Manage differents error cases.
            if (
                response.status_code == 404
                and "Unknown email campaign id" in json.loads(response.text)["message"]
            ):
                return None
            else:
                print(f"Endpoint: {endpoint}")
                print(f"{response.reason} - {response.text}")
                raise ("Error while processing request")

        response = response.json()
        if "ServiceErrorCode" in response:
            print(f'ServiceErrorCode: {response["ServiceErrorCode"]}')
            return None

        return response

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
