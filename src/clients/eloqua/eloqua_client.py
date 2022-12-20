import base64
import json
import time
from threading import current_thread, get_ident, get_native_id

from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict

from src.clients.aws.aws_tools import Secret, search_secrets_by_prefix
from src.commons.client import Client
from src.utils.endpoint_utils import Endpoint
from src.utils.http_utils import get_http_adapter
from src.utils.various_utils import get_chunks, run_in_threads_pool

ELOQUA_CREDFENTIALS_SECRET_NAME = 'ingest-framework/production/eloqua/api-credentials'


class EloquaEndpoint(Endpoint):
    def __init__(
        self,
        params=None,
        url_template=None,
        query_params=None,
        credentials=None,
        client_name=None
    ):
        super().__init__(
            params=params, url_template=url_template, query_params=query_params
        )
        self.credentials = credentials
        self.client_name = client_name
        self._headers = None

    def build_base64_basic_auth(self):
        auth_string = f"{self.client_name}\{self.credentials}"
        bytes_auth_string = auth_string.encode('utf-8')
        base64_auth_string = base64.b64encode(bytes_auth_string)
        return base64_auth_string.decode('utf-8')

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
        ] = f"Basic {self.build_base64_basic_auth()}"

        return headers

    @property
    def headers(self):
        if self._headers is None:
            headers = self.build_headers()
            self._headers = headers
        return self._headers


def pagination_function(endpoint, task_result):
    elements = task_result.get("elements")
    if not elements:
        return None
    pagination_param = endpoint.get_param_by_name("page")
    pagination_param.value = task_result["page"] + 1
    return endpoint


class EloquaClient(Client):
    def __init__(self, task=None, env=None, db_connection=None):
        super().__init__(env)
        self.task = task
        self.http_adapter = get_http_adapter()
        self.db_connection = db_connection

    @staticmethod
    def get_client_name(dbparams):
        try:
            client_name = list(dbparams.values())[0]["client_name"]
            return client_name
        except:
            raise("Can't find client_name in dbparams. THis param is mandatory for Eloqua.")

    @staticmethod
    def get_client_credentials_from_aws_secrets():
        secret_value = Secret(ELOQUA_CREDFENTIALS_SECRET_NAME).get_value()
        return secret_value["credentials"]

    def get(self, task_params, **_ignored):

        # Construire les db_params
        db_params = self.get_request_params()

        credentials = self.get_client_credentials_from_aws_secrets()

        request_params = [
            {
                k: {
                    "endpoint": EloquaEndpoint(
                        params=v,
                        url_template=task_params["query"]["template"],
                        query_params=task_params["query"],
                        credentials=credentials,
                        client_name=v["client_name"]
                    )
                }
            }
            for k, v in db_params.items()
        ]
        result = []

        print(f"Number of requests to run: {len(request_params)}")

        api_datas = []

        endpoint_list_list = get_chunks(request_params, chunk_size=100)
        for lst in endpoint_list_list:
            print(f"Chunck with {len(lst)} queries")

            chunks_result_list = run_in_threads_pool(
                request_params_list=lst,
                source_function=self.do_get_query,
                result_key=task_params["query"]["response_datas_key"],
                pagination_function=pagination_function
            )
            api_datas.extend(chunks_result_list)

        result = self.add_request_params_to_api_call_result(
            api_datas, task_params, db_params
        )

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
                f"{cpt} attemp(s) failed. Restarting thread after 100 seconds "
                f"of pause: name={c_thread.name}, idnet={get_ident()}, "
                f"id={get_native_id()}"
            )
            response = self.do_get_query(endpoint=endpoint, headers=headers)
            if cpt == 5:
                raise TimeoutError(f"Failed to reach endpoint: {endpoint}")

            return None

        if response.status_code != 200 and response.status_code != 401:
            # TODO: Manage differents error cases.
            if (
                response.status_code == 404
                and "Unknown email campaign id"
                in json.loads(response.text)["message"]  # noqa: E501
            ):
                return None
            else:
                raise Exception(
                    f"Error while processing request: endpoint: {endpoint} "
                    f"response: {response.reason} - {response.text}"
                )

        response = response.json()
        if "ServiceErrorCode" in response:
            print(f'ServiceErrorCode: {response["ServiceErrorCode"]}')
            return None

        return (response, endpoint)
