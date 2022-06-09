import json
import time
from datetime import datetime
from urllib.parse import urlencode

import requests
from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict
from src.clients.aws.aws_tools import Secret
from src.commons.client import Client
from src.utils.http_utils import get_http_adapter


class LinkedInAccessToken:
    name = "/linkedin/ads/api/access-token"

    def __init__(self):
        self.refresh_token_value = LinkedInRefreshToken().value
        self.client_secret_value = LinkedInClientCredentials().get_value()

    # @property
    # def will_expire_soon(self):
    #     endpoint = "https://www.linkedin.com/oauth/v2/introspectToken"
    #     data = {
    #         "client_id": self.client_secret_value["id"],
    #         "client_secret": self.client_secret_value["secret"],
    #         "token": access_token,
    #     }

    #     response = self.do_query(endpoint=endpoint, data=data)
    #     expires_at = json.loads(response.text)["expires_at"]

    #     if expires_at < datetime.now().timestamp() + (3 * 24 * 3600):
    #         return True

    #     return False

    @property
    def value(self):

        endpoint = "https://www.linkedin.com/oauth/v2/accessToken"

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token_value,
            "client_id": self.client_secret_value["id"],
            "client_secret": self.client_secret_value["secret"],
        }

        response = LinkedInClient.do_auth_post_query(endpoint=endpoint, data=data)

        access_token = response.json()["access_token"]

        if not access_token:
            raise TypeError(
                "Wrong type for access_token. Should be str got"
                f" {type(access_token)} instead"
            )
        return access_token


class LinkedInClientCredentials(Secret):
    name = "linkedin/ads/api/client"

    def __init__(self):
        super().__init__(self.name)

    @property
    def value(self):
        return self.get_value()


class LinkedInRefreshToken(Secret):
    name = "linkedin/ads/api/refresh-token"

    def __init__(self):
        super().__init__(self.name)
        self.client_secret_value = LinkedInClientCredentials().get_value()

        # Launch Sentry alert. Refresh-token can only be refreshed manually. 1 year TTL.
        if self.will_expire_soon:
            # TODO: Launch alert on Sentry
            print("LinkedIn API Refresh token will expire soon. Please renew it)")

    @property
    def value(self):
        return self.get_value()["value"]

    @property
    def will_expire_soon(self):
        endpoint = "https://www.linkedin.com/oauth/v2/introspectToken"
        data = {
            "client_id": self.client_secret_value["id"],
            "client_secret": self.client_secret_value["secret"],
            "token": self.value,
        }

        response = LinkedInClient.do_auth_post_query(endpoint=endpoint, data=data)
        expires_at = json.loads(response.text)["expires_at"]

        if expires_at < datetime.now().timestamp() + (7 * 24 * 3600):
            return True

        return False


class LinkedInClient(Client):
    """# noqa: E501
    This object is responsbile of:
    - Building API http requests
    - Querying the API
    - Filtering response datas
    - Transforming datas
    - Sending result to the task for insertion or update.

    Attributes:
        acces_token: str
            The linkedIn access token.
        destination: inst RedShiftClient
            Used to instantiate Model for retrieving existing values in DB for filtering. Bad design here.
            #TODO: Cleaner design. Find another logic than instatiating Model for retrieving existing in Db.

    """

    def __init__(self, task=None, env=None):
        super().__init__(env)
        self.access_token = LinkedInAccessToken().value
        self.task = task
        self.http_adapter = get_http_adapter()

    def get_chunks(self, lst):
        chunk_size = 500
        if len(lst) > chunk_size:
            results_lists = [
                lst[offs : offs + chunk_size] for offs in range(0, len(lst), chunk_size)
            ]
            return results_lists
        else:
            return [lst]

    def get(self, task_params, header=None):
        """# noqa: E501
        Build the endpoint, query the API.

        Build a dict with API response AND add an entry with values from other table if necessary

        Args:
            task_params: dict
                THe dictionary of the task params as imperted from the json definition.

        Returns:
            A list of dicts.
        """
        headers = self.build_headers(header=header)
        url_params = task_params["url"]
        params = url_params.get("params", None)

        if params:
            db_params = self.get_request_params(self.task)
            total_requests_number = len(db_params)
            dynamics_params = self.get_dynamics_params(params)
            statics_params = self.get_statics_params(params)
            kwargs = dynamics_params + statics_params
            endpoint_list = [
                (
                    self.build_endpoint(
                        base=url_params["base"],
                        category=url_params["category"],
                        q=url_params["q"],
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
                        category=url_params["category"],
                        q=url_params["q"],
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
                    print(endpoint)
                    threads.append(
                        (
                            executor.submit(self.do_get_query, endpoint[0], headers),
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

    def build_headers(self, header=None):
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
        headers["Authorization"] = f"Bearer {self.access_token}"

        if header:
            headers.update(header)

        return headers

    def build_endpoint(self, base=None, category=None, q=None, kwargs=None, args=None):
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
            f"{category if category else ''}/"
            f"{str(args[0])+'?' if args else ''}"
            f"{'?q='+q if q else ''}"
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
        try:
            response = self.http_adapter.get(url=endpoint, headers=headers)
        except ConnectionError as e:
            print("Error while connecting to db")
            print(e)
            return None
        except ConnectTimeout as e:
            print("Timeout connecting to db")
            print(e)
            return None
        except RetryError as e:
            print("Timeout connecting to db")
            print(e)
            import time

            print("429 limit reached. Wait 5 minutes.")
            time.sleep(300)
            print("Restarting after 300 seconds of pause")
            return None
        except Exception as e:
            print("Unhandled exception occurs")
            print(e)
            return None

        if response.status_code != 200:
            print("Error while processing request")
            print(f"Endpoint: {endpoint}")
            print(f"{response.reason} - {response.text}")
            # TODO: Manage differents error cases.
            return None

        response = response.json()
        if "ServiceErrorCode" in response:
            print(f'ServiceErrorCode: {response["ServiceErrorCode"]}')
            return None

        return response

    @staticmethod
    def do_auth_post_query(
        endpoint=None,
        data=None,
        headers=None,
    ):
        """# noqa: E501
        Qurey the API endpoint with a POST query for auth

        Args:
            endpoint: str
                The endpoint
            data:

            headers:

        Returns:
            A json str repeenting the API response
        """
        response = requests.post(url=endpoint, data=data, headers=headers)

        if response.status_code != 200:
            # TODO: Manage differents error cases.
            print("Error while processing request")

        return response


if __name__ == "__main__":
    lc = LinkedInClient()
    print(lc)
