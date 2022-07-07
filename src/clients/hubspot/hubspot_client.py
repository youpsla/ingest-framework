import concurrent.futures
import json
import time
from gc import callbacks
from threading import current_thread, get_ident, get_native_id
from urllib.parse import urlencode

from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict
from src.clients.aws.aws_tools import Secret, search_secrets_by_prefix
from src.commons.client import Client
from src.utils.http_utils import get_http_adapter
from src.utils.various_utils import get_chunks

from hubspot import HubSpot
from hubspot.auth.oauth.api.tokens_api import TokensApi
from hubspot.crm.associations import BatchInputPublicObjectId, PublicObjectId
from hubspot.crm.companies.models.simple_public_object_with_associations import (
    SimplePublicObjectWithAssociations as companies_public_object,
)
from hubspot.crm.contacts.models.simple_public_object_with_associations import (
    SimplePublicObjectWithAssociations as contacts_public_object,
)
from hubspot.events.models.external_unified_event import ExternalUnifiedEvent

refresh_token_params = {
    "grant_type": "refresh_token",
    "client_id": "fa6040db-72c2-4cca-b529-34d951716062",
    "client_secret": "f5d8dedb-7d18-4c19-a2a7-d25053d9c6cb",
    "redirect_uri": "https://connect.jabmo.app",
    "refresh_token": "9c2cb2f5-b460-4bea-b1a1-b1df862bc3c1",
}


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

    def print_result(result):
        print(result)

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
                (
                    tmp_secret_value["portal_id"],
                    tmp_secret_value["access_token"],
                    tmp_secret_value["refresh_token"],
                )
            )

        # ptl = [(25912207, 'CLWs0tybMBIMggMAUAAAACAAAABIGI_HrQwghercFSj5iTgyFAncLX84DUTpinSCszbkBvScEXT4OjAAMWBB_wcAADwAtABg4HzOKIYAAGAAACA8ACAYAAAAwMN_NgEAAACBZxwY4AAAIAJCFMz1adgaAs5ngxV6JC62tGxh1YwMSgNldTFSAFoA'), ('25955882', 'CKi709ybMBIMggMAUAAAACAAAABIGKqcsAwgioLPDSj5iTgyFL8KPuPP69akxB2E6Rb-7K2WVw5TOjAAMWBB_wcAADwAtABg4HzOKIYAAGAAACA8ACAYAAAAwMN_NgEAAACBZxwY4AAAIAJCFFqrVj3APiWCbyvmw05zSi_EhpQySgNldTFSAFoA'), (1838475, 'CIab1dybMBIMggMAUAAAACAAAABIGIubcCDywqgNKPmJODIUWaoWClovooFugJn6lz4EhyJzb986MAAxYEH_BwAAHAC0AGDgfMYohgAAIAAAABwAIBgAAADAw382AQAAAIFnHBjAAAAgAkIU-PYny8M0Zbo9XURWCQr5O9lJ3MlKA25hMVIAWgA')]
        # portal_token_list = ptl[0:2]

        for ptd in [portal_token_list[2]]:
            # for ptd in portal_token_list:
            access_token = ptd[1]
            portal_id = ptd[0]
            oauth_api_client = HubSpot(access_token=access_token)
            result = []

            tmp = None

            if self.task.name == "ccccccccontacts":
                properties = (
                    [
                        "email",
                        "firstname",
                        "lastname",
                        "job_title",
                        "company",
                        "industry",
                        "hs_analytics_average_page_views",
                        "contact_owner",
                        "address",
                        "zip",
                        "city",
                        "region",
                        "country",
                        "hs_email_domain",
                        "hs_analytics_num_event_completions",
                        "ip_city",
                        "ip_country",
                        "ip_country_code",
                        "ip_state",
                        "ip_state_code",
                        "createdAt",
                        "updated_at",
                        "portal_id",
                    ],
                )
                result = oauth_api_client.crm.contacts.get_all(properties=properties)

            if self.task.name == "contacts":
                properties = [
                    "email",
                    "firstname",
                    "lastname",
                    "job_title",
                    "company",
                    "industry",
                    "hs_analytics_average_page_views",
                    "contact_owner",
                    "address",
                    "zip",
                    "city",
                    "region",
                    "country",
                    "hs_email_domain",
                    "hs_analytics_num_event_completions",
                    "ip_city",
                    "ip_country",
                    "ip_country_code",
                    "ip_state",
                    "ip_state_code",
                    "createdAt",
                    "updated_at",
                    "portal_id",
                ]

                has_to_continue = True
                result = []
                after = 0
                while has_to_continue:

                    r = oauth_api_client.crm.contacts.basic_api.get_page(
                        properties=properties, limit=100, after=after
                    )
                    result.extend(r.results)
                    if r.paging is None:
                        has_to_continue = False
                    else:
                        print(len(result))
                        after = r.paging.next.after
                        continue
                print(f"#contacts for portal_id {portal_id}: {len(result)}")

            if self.task.name == "companies":
                properties = [
                    "createdAt",
                    "updatedAt",
                    "domain",
                    "name",
                    "hs_additional_domains",
                    "hs_analytics_num_page_views",
                    "hs_analytics_num_visits",
                    "hs_is_target_account",
                    "hs_object_id",
                    "num_associated_contacts",
                    "website",
                    "hs_parent_company_id",
                    "archived",
                ]
                has_to_continue = True
                result = []
                after = 0
                while has_to_continue:

                    r = oauth_api_client.crm.companies.basic_api.get_page(
                        properties=properties, limit=100, after=after
                    )
                    result.extend(r.results)
                    if r.paging is None:
                        has_to_continue = False
                    else:
                        print(len(result))
                        after = r.paging.next.after
                        continue

            # if self.task.name == "email_events":
            #     properties = (
            #         [
            #             "createdAt",
            #             "updatedAt",
            #             "domain",
            #             "name",
            #             "hs_additional_domains",
            #             "hs_analytics_num_page_views",
            #             "hs_analytics_num_visits",
            #             "hs_is_target_account",
            #             "hs_object_id",
            #             "num_associated_contacts",
            #             "website",
            #             "hs_parent_company_id",
            #             "archived",
            #         ],
            #     )
            #     has_to_continue = True
            #     result = []
            #     after = 0
            #     while has_to_continue:
            #         r = oauth_api_client.crm.objects.emails.basic_api.get_page(
            #             limit=100,
            #             **param[1][0],
            #             **param[1][1],
            #         )
            #         r = oauth_api_client.crm.companies.basic_api.get_page(
            #             properties=properties, limit=100, after=after
            #         )
            #         result.extend(r.results)
            #         if r.paging is None:
            #             has_to_continue = False
            #         else:
            #             print(len(result))
            #             after = r.paging.next.after
            #             continue

            # print(len(r))
            # result = oauth_api_client.crm.companies.get_all(
            #     properties=[
            #         "createdAt",
            #         "updatedAt",
            #         "domain",
            #         "name",
            #         "hs_additional_domains",
            #         "hs_analytics_num_page_views",
            #         "hs_analytics_num_visits",
            #         "hs_is_target_account",
            #         "hs_object_id",
            #         "num_associated_contacts",
            #         "website",
            #         "hs_parent_company_id",
            #         "archived",
            #     ]
            # )
            # print(len(result))

            if self.task.name in ["events"]:
                db_params = self.get_request_params(self.task)
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
            # if tmp:
            #     for t in tmp:
            #         if isinstance(t, (companies_public_object, contacts_public_object)):
            #             t = t.to_dict()
            #         t["portal_id"] = portal_id
            #         result.append(t)
            # else:
            #     pass

            # if self.task.name == "company_contact_associations":

            #     db_params = self.get_request_params(self.task)
            #     total_requests_number = len(db_params)
            #     # dede = oauth_api_client.crm.associations.types_api.get_all(from_object_type="deals", to_object_type="companies")
            #     big_result = []
            #     for params in db_params:
            #         batch_input_public_object_id = BatchInputPublicObjectId(
            #             inputs=[PublicObjectId(id=params[0][0]["company_id"])]
            #         )
            #         has_to_continue = True
            #         result = []
            #         offset = 0
            #         while has_to_continue:

            #             r = oauth_api_client.crm.associations.batch_api.read(
            #                 from_object_type="COMPANIES",
            #                 to_object_type="CONTACTS",
            #                 batch_input_public_object_id=batch_input_public_object_id,
            #             )
            #             result.extend(r.results)
            #             # if r.hasMore is False:
            #             #     has_to_continue = False
            #             # else:
            #             #     print(len(result))
            #             #     offset = r.offset
            #             #     continue

            if self.task.name in ["company_contact_associations"]:

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
                                kwargs=v[1] + kwargs,
                                args=v[2],
                            ),
                            k,
                        )
                        for k, v in db_params.items()
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

                futures_results = []
                response_key = url_params.get("response_datas_key", None)
                endpoint_list_list = get_chunks(endpoint_list)
                # endpoint_list_list = [
                #     [
                #         (
                #             "https://api.hubapi.com/crm-associations/v1/associations/817666151/HUBSPOT_DEFINED/2",
                #             [{"company_id": "817666151"}],
                #         )
                #     ]
                # ]

                for lst in endpoint_list_list:
                    access_token = (
                        TokensApi().create_token(**refresh_token_params).access_token
                    )
                    headers = self.build_headers(header=None, access_token=access_token)

                    print(f"Chunck with {len(lst)} queries")

                    from src.utils.various_utils import run_in_threads_pool

                    chunks_result_list = run_in_threads_pool(
                        request_params_list=lst,
                        source_function=self.do_get_query,
                        headers=headers,
                    )

                    def add_source_params_to_result(db_params=None, chunk=None):
                        result = {}
                        for chunk_result in chunks_result_list:
                            for k, v in chunk_result.items():
                                result[k] = v + db_params[k]
                            return result

                    # Adding source params to the result
                    for chunk in chunks_result_list:
                        dudu = add_source_params_to_result(
                            db_params=db_params, chunk=chunk
                        )
                        print(dudu)

                    local_result = []
                    if task[1]:
                        for r in response_elements:
                            for f in task[1]:
                                for k, v in f.items():
                                    local_result.append({k: v, "contact_id": r})
                    if len(local_result) > 0:
                        futures_results.append(local_result)

                for r in futures_results:
                    if r:
                        result.extend(r)

            if self.task.name in [
                "campaign_details",
                "campaigns",
                "email_events",
                # "company_contact_associations",
            ]:

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
                endpoint_list_list = get_chunks(endpoint_list)
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
                    if isinstance(
                        r,
                        (
                            companies_public_object,
                            contacts_public_object,
                            ExternalUnifiedEvent,
                        ),
                    ):
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
