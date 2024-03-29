import json
import time
from datetime import datetime
from threading import current_thread, get_ident, get_native_id

import requests
from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict

from src.clients.aws.aws_tools import Secret
from src.commons.client import Client
from src.utils.endpoint_utils import Endpoint
from src.utils.http_utils import get_http_adapter


class LinkedInAccessToken:
    name = "/linkedin/ads/api/access-token"

    def __init__(self):
        self.refresh_token_value = LinkedInRefreshToken().value
        self.client_secret_value = LinkedInClientCredentials().get_value()

    @property
    def value(self):

        endpoint = "https://www.linkedin.com/oauth/v2/accessToken"

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token_value,
            "client_id": self.client_secret_value["id"],
            "client_secret": self.client_secret_value["secret"],
        }

        response = LinkedInClient.do_auth_post_query(
            endpoint=endpoint, data=data)

        # WIth the new API, looks like the generated token is not fully processed enough quick by linkedin.
        # A small sleep here allow Linkedin to have time to proceed.
        time.sleep(2)

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

        response = LinkedInClient.do_auth_post_query(
            endpoint=endpoint, data=data)
        expires_at = json.loads(response.text)["expires_at"]

        if expires_at < datetime.now().timestamp() + (7 * 24 * 3600):
            return True

        return False


class LinkedinEndpoint(Endpoint):
    def __init__(
        self,
        params=None,
        url_template=None,
        query_params=None,
        access_token=None,
    ):
        super().__init__(
            params=params,
            url_template=url_template,
            query_params=query_params,
        )
        self.access_token = access_token
        self._headers = None

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

        # Add custom header from task params
        task_headers = self.query_params.get("request_header")
        if task_headers:
            headers.update(task_headers)

        if header:
            headers.update(header)

        return headers

    @property
    def headers(self):
        if self._headers is None:
            headers = self.build_headers()
            self._headers = headers
        return self._headers


def paging_pagination(endpoint, task_result):
    start = task_result["paging"]["start"]
    total = task_result["paging"]["total"]
    count = task_result["paging"]["count"]
    if start >= total:
        return None
    pagination_param = endpoint.get_param_by_name("start")
    pagination_param.value = start + count
    return endpoint


class LinkedInClient(Client):
    """# noqa: E501
    Inerface to the provider.

    The get() method is called when running task

    Parameters:
        task: Task object
        env: Env to run on.

    Attributes:
        acces_token: str
            The linkedIn access token.

    """

    def __init__(self, task=None, env=None):
        super().__init__(env)
        self.access_token = LinkedInAccessToken().value
        self.task = task
        self.http_adapter = get_http_adapter()

    def get(self, task_params, header=None):
        """# noqa: E501
        Do all necessary actions to retrieve datas from provider. THose actions can be:
        - Getting Auth
        - Get requests params from task.json
        - Build enpoints_list from params
        - Querying the provider for data
        - Adding custom data to APi call result.

        Build a dict with API response AND add an entry with values from other table if necessary

        Args:
            task_params: dict
                THe dictionary of the task params as imported from the json definition.

        Returns:
            A list of lists.
        """

        # Get a list of params to use for requests.
        db_params = self.get_request_params()
        print(f"Number of requests to run: {len(db_params)}")

        request_params = [
            {
                k: {
                    "endpoint": LinkedinEndpoint(
                        params=v,
                        url_template=task_params["query"]["template"],
                        query_params=task_params["query"],
                        access_token=self.access_token,
                    )
                }
            }
            for k, v in db_params.items()
        ]
        result = []

        # Request provider. and get result of all requests.
        if self.task.name not in [
            "creative_sponsored_video_daily_update",
            "daily_social_metrics_update",
            "creative_sponsored_video__creative_name_daily_update",
            "creative_sponsored_update_daily_update",
            "creative_url_daily_update",
            "account_pivot_campaign_daily_update",
            "pivot_member_country_monthly_update",
            "pivot_member_county_monthly_update",
            "pivot_job_title_monthly_update",
            "pivot_member_organization_monthly_update",
            "organizations_monthly_update",
            "creative_url_3_months_daily_update",
        ]:
            pagination_function = paging_pagination
        else:
            pagination_function = None

        api_datas = self.do_requests(
            task_params, request_params, pagination_function)

        # Add data to the API response
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
            response = self.do_get_query(endpoint=endpoint)
            if cpt == 5:
                raise TimeoutError("Failed to reach endpoint: {endpoint}")

            return None

        if response.status_code != 200 and response.status_code != 401:
            # TODO: Manage differents error cases.
            if (
                response.status_code == 404
                and "Unknown email campaign id"
                in json.loads(response.text)["message"]
            ):
                return None
            # When a creative doesn't exist anymore in the Linkedin DB, got a
            # 404 not found error.
            elif response.status_code == 404:
                print("Creative not found in Linkedin DB")
            else:
                raise Exception(
                    f"Error while processing request Endpoint: {endpoint} "
                    f"{response.reason} - {response.text}"
                )

        response = response.json()
        if "ServiceErrorCode" in response:
            print(f'ServiceErrorCode: {response["ServiceErrorCode"]}')
            return None

        return (response, endpoint)

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
