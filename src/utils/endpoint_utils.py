from urllib.parse import urlencode

from requests.structures import CaseInsensitiveDict


class Endpoint:
    def __init__(
        self,
        params=None,
        query_params=None,
        url_template=None,
        access_token=None,
        e_type=None,
        task_params=None,
    ):
        self.params = params
        self.query_params = query_params
        self.e_type = e_type
        self.access_token = access_token
        self.url_template = url_template
        self.task_params = task_params
        self._endpoint_param_object_list = []

    @property
    def endpoint_param_object_list(self):
        if not self._endpoint_param_object_list:
            for k, v in self.params.items():
                # if self.params
                self._endpoint_param_object_list.append(
                    EndpointParam(
                        endpoint_params={k: v}, query_params=self.query_params
                    )
                )
        return self._endpoint_param_object_list

    @staticmethod
    def get_kwargs_encoded(kwargs):
        kwargs_tuple = [(k, v) for f in kwargs for k, v in f.items()]
        return urlencode(kwargs_tuple)

    def get_endpoint_as_string(self):
        args_dict = {}
        for arg in self.endpoint_param_object_list:
            args_dict.update(arg.get_as_dict_url_formatted())
        return self.url_template.format(**args_dict)

    def get_param_by_name(self, name):
        for elem in self.endpoint_param_object_list:
            if elem.name == name:
                return elem
        return None

    def get_headers(self, header=None, access_token=None):
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


class EndpointParam:
    def __init__(self, endpoint_params=None, query_params=None):
        self.endpoint_params = endpoint_params
        self.query_params = query_params

        for k, v in self.endpoint_params.items():
            self.name = k
            self.value = v

        for qp in self.query_params:
            if qp["name"] == self.name:
                for k, v in qp.items():
                    setattr(self, k, v)

        if not hasattr(self, "value"):
            raise ValueError(
                "Error while EndpointParam init. No value found. Value must be in json params file or a source_value should be given"
            )

    @property
    def formatted_value(self):
        if hasattr(self, "template"):
            return self.template.format(self.value)
        return self.value

    def __repr__(self):
        return f"EndpointParam {self.name}/{self.value}"

    def get_as_dict_url_formatted(self):
        if self.output_type == "arg":
            url_str = self.formatted_value
        if self.output_type == "kwarg":
            url_str = urlencode({self.name: self.formatted_value})
        return {self.name: url_str}

    def get_as_url_encoded(self):
        if self.output_type == "arg":
            url_str = self.formatted_value
        if self.output_type == "kwarg":
            url_str = urlencode({self.name: self.formatted_value})
        return url_str

    def get_as_dict(self):
        return {self.name: self.get_formatted_value()}


if __name__ == "__main__":
    import json

    task = """
        {"company_contact_associations": {
            "actions": [
                "insert"
            ],
            "source": "hubspot",
            "destination": "redshift",
            "request_data_source": "redshift",
            "model": "company_contact_associations",
            "data_source": {
                "name": "campaigns",
                "raw_sql": "select distinct(id) from {schema}.companies"
            },
            "fields_to_add_to_api_result": [
                {
                    "name": "company_id",
                    "origin_key": "id",
                    "destination_key": "company_id"
                }
            ],
            "query": {
                "e_type": "rest",
                "template": "{base}?&{limit}&{offset}&{startTimestamp}&{endTimestamp}",
                "response_datas_key": "results",
                "base": "https://api.hubapi.com/crm-associations/v1/associations/",
                "params": [
                    {
                        "name": "base",
                        "output_type": "arg",
                        "value": "https://api.hubapi.com/crm-associations/v1/associations/"
                    },
                    {
                        "data_source": "campaigns",
                        "name": "company_id",
                        "source_key": "id",
                        "output_type": "arg"
                    },
                    {
                        "name": "limit",
                        "output_type": "kwarg",
                        "value": 100
                    },
                    {
                        "name": "appId",
                        "output_type": "kwarg",
                        "value": ""
                    },
                    {
                        "name": "offset",
                        "output_type": "kwarg",
                        "value": 0
                    },
                    {
                        "name": "startTimestamp",
                        "type": "timestamp_from_epoch",
                        "offset_type": "from_today",
                        "offset_value": "1",
                        "offset_unity": "days",
                        "position": "start",
                        "output_type": "kwarg"
                    },
                    {
                        "name": "endTimestamp",
                        "type": "timestamp_from_epoch",
                        "offset_type": "from_today",
                        "offset_value": "1",
                        "offset_unity": "days",
                        "position": "end",
                        "output_type": "kwarg"
                    }
                ]
            }
        }
        }
        """

    db_params_1 = {
        "endTimestamp": 1658267999,
        "startTimestamp": 1658181600,
        "offset": "",
        "limit": 1000,
        "base": "https://api.hubapi.com/email/public/v1/events",
    }

    task = json.loads(task)
    query = task["company_contact_associations"]["query"]

    edp = Endpoint(
        params=db_params_1,
        url_template=query["template"],
        query_params=query["params"],
    )

    print(edp)
    print(edp.get_endpoint_as_string())
