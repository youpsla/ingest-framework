import json
from datetime import date, datetime, timedelta
from urllib.parse import urlencode

import dateutil.relativedelta
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.structures import CaseInsensitiveDict

from aws_tools import Secret
from models import Model
from utils.http_utils import get_http_adapter


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


class LinkedInClient:
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

    def __init__(self, destination=None):
        self.access_token = LinkedInAccessToken().value
        self.destination = destination
        self.http_adapter = get_http_adapter()

    def get_dynamics_param(self, name, params, value):
        """# noqa: E501
        Build a dict for an endpoint kwarg. Envetually, transform the data collected from destination (Db).
        At teh moment, only kwargs of type "date" are manage. Return None in other cases.

        Args:
            name: str
            The name /key of kwarg

            params: dict
                kwarg description as found in task.json

            value: str or int
                The value retrieve from source

        Returns:
            a dict {name:value}
            None is this kwargs is not of type "date".

        """
        if params["value_type"] == "date":
            return {
                name: params["url_query_parameter_value"].format(
                    day=value.day,
                    month=value.month,
                    year=value.year,
                ),
            }

        return None

    def get_dynamics_group_params(self, params):
        """# noqa: E501
        Manage group fo params. Group fo params. It is used to set start and end date for http API queries. AS the offset_parameter is common to day, month, year I group them in a group.

        Use offset_unity, offset_value found in params to calculate the date for, http request.
        Args:
            params: dict

        Returns:
            a dict {name:value}
            None is this kwargs is not of type "date".
        """
        url_params = params["url_params"]
        today = datetime.today()
        result = []
        if params["offset_unity"] == "days":
            tmp = {params["offset_unity"]: int(params["offset_value"])}
            start_date = today - timedelta(**tmp)
            for k, v in url_params.items():
                result.append(self.get_dynamics_param(k, v, start_date))

        if params["offset_unity"] == "months":

            today = date.today()
            last_day_of_prev_month = (
                today
                - dateutil.relativedelta.relativedelta(
                    months=int(params["offset_value"]) - 1
                )
            ).replace(day=1) - timedelta(days=1)

            start_day_of_prev_month = last_day_of_prev_month.replace(day=1)

            for k, v in url_params.items():
                result.append(
                    self.get_dynamics_param(
                        k,
                        v,
                        start_day_of_prev_month
                        if params["offset_range_position"] == "start_day"
                        else last_day_of_prev_month,
                    )
                )

        return result

    def get_kwargs_list(self, kwargs_fields=[], sql_datas=[], urlencode=False):
        """# noqa: E501
        Manage group fo params. Group fo params. It is used to set start and end date for http API queries. AS the offset_parameter is common to day, month, year I group them in a group.

        Use offset_unity, offset_value found in params to calculate the date for, http request.
        Args:
            params: dict

        Returns:
            a dict {name:value}
            None is this kwargs is not of type "date".
        """
        result = []
        for d in sql_datas:
            tmp_result = []
            for f in kwargs_fields:
                r = {f[2]: f[1].format(d[f[0]])}
                tmp_result.append(r)
            result.append(tmp_result)

        return result

    def get_args_list(self, args_fields=[], sql_datas=[]):

        """# noqa: E501
        Build the list of args for the endpoint http requets

        Args:
            args_fields: list
                List of args fields
            sql_datas: list
                List of datas retrieved from db


        Returns:
            a list which elements are string ef values to insert in url.
            The returned list can be empty.
        """

        result = []
        if not args_fields:
            return args_fields
        for d in sql_datas:
            tmp_result = []
            for f in args_fields:
                tmp_result.append(d[f])
            result.append(tmp_result)

        return result

    def get_sql_list(self, db_fields=[], sql_datas=[]):
        """# noqa: E501
        In some table, we musy insert datas from other tables, datas not available in the API response.
        For example, the "pivot_job_title_monthly_update" needs a "facet_id" field. This data is used for querying the source API BUT need to be writtten with anohter column name in another table.
        We collect the "campaign_id" for filtering API and we must write this information in the "facet_id" column.

        Args:
            db_fields: list
                List of dict. The form of each dict is: {"origin_key": name_of_the_origin_column, "target_key": name_of_the_destination_column}
            sql_datas: list
                List of dict. The form of each dict is:: {"name_of_origin_column": value}


        Returns:
            a list of dicts. The form of each dict is:: {"name_of_target_column": value}
            The returned list can be empty.
        """
        result = []
        if not db_fields:
            return db_fields
        for d in sql_datas:
            tmp_result = []
            for f in db_fields:
                tmp_result.append({f["destination_key"]: d[f["origin_key"]]})
            result.append(tmp_result)

        return result

    def get_filter_values_from_db(self, params=None):
        """# noqa: E501
        Build 3 list of dicts:
        - kwargs_list: A list of dicts containing key and value for building endpoint kwargs
        - args_list: A list of strings containing key and value for building endpoint args
        - sql_list: A list of dicts containing key and value for writing in Db values retirieved from Db.

        Args:
            params: list
                List of params as defined in task.json

        Returns:
            3 lists as explained above
        """
        if not params:
            return [], [], []

        for v in params.values():
            if "rawsql" in v["type"]:
                tmp = Model.get_from_raw_sql(self.destination, v["raw_sql"])
            else:
                model = Model(v["filter_model"], db_engine=self.destination)
                tmp = model.get_all(fields=v["all_fields"])

            kwargs_list = self.get_kwargs_list(v["kwargs_fields"], tmp)
            args_list = self.get_args_list(v["args_fields"], tmp)
            sql_list = self.get_sql_list(v["db_fields"], tmp)

        return kwargs_list, args_list, sql_list

    def get_dynamics_params(self, params):
        """# noqa: E501
        Build dynamics params from json

        Args:
            params: list
                List of params as defined in task.json

        Returns:
            A list of dicts. Each dict has the form: {"kwargs_name": value}
        """
        dynamics_params = params.get("dynamics", {})
        result = []
        for n, p in dynamics_params.items():
            if p["type"] == "group":
                result += self.get_dynamics_group_params(p)
            else:
                result += self.get_dynamics_param(n, p)

        return result

    def get_statics_params(self, params):
        """# noqa: E501
        Build statics params from json

        Args:
            params: list
                List of params as defined in task.json

        Returns:
            A list of dicts. Each dict has the form: {"kwargs_name": value}
        """
        return [{k: v} for k, v in params.get("statics", {}).items()]

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
            kwargs_list, args_list, sql_list = (
                self.get_filter_values_from_db(params.get("db", None))
                if params
                else ([], [], [])
            )

            dynamics_params = self.get_dynamics_params(params)
            statics_params = self.get_statics_params(params)

            kwargs = dynamics_params + statics_params

            from itertools import zip_longest

            zip_datas = list(zip_longest(sql_list, kwargs_list, args_list))
            # zip_datas = list(zip_longest(sql_list, kwargs_list, args_list))[0:2]
            result = []
            for zd in zip_datas:
                endpoint = self.build_endpoint(
                    base=url_params["base"],
                    category=url_params["category"],
                    q=url_params["q"],
                    kwargs=zd[1] + kwargs,
                    args=zd[2],
                )

                data = self.do_get_query(endpoint=endpoint, headers=headers)
                # If no data continue to next iteration
                if not data:
                    continue

                response_key = url_params.get("response_datas_key", None)

                tmp_result = []
                if response_key:
                    data = data[response_key]
                    print(len(data))
                    if len(data) >= 15000:
                        print(
                            "LINKEDIN API error. Max elements of 15 000 per request"
                            f" reached. Elments for enpoint {endpoint} will not be"
                            " inserted in Db."
                        )
                        continue
                    for da in data:
                        tmp_result.append(da)
                else:
                    tmp_result.append(data)

                if zd[0]:
                    for r in tmp_result:
                        for f in zd[0]:
                            for k, v in f.items():
                                r[k] = v

                result += tmp_result

        else:
            endpoint = self.build_endpoint(
                base=url_params["base"],
                category=url_params["category"],
                q=url_params["q"],
            )

            data = self.do_get_query(endpoint=endpoint, headers=headers)
            # If no data continue to next iteration
            if not data:
                return None

            response_key = url_params.get("response_datas_key", None)
            tmp_result = []
            if response_key:
                data = data[response_key]
                for d in data:
                    tmp_result.append(d)
            else:
                tmp_result.append(data)

            result = tmp_result

        return [{"datas": d} for d in result]

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
