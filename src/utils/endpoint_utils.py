from urllib.parse import quote_plus, urlencode

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
        headers["Authorization"] = f"Bearer {access_token or self.access_token}"

        if header:
            headers.update(header)

        return headers


class EndpointParam:
    def __init__(self, endpoint_params=None, query_params=None):
        self.endpoint_params = endpoint_params
        self.query_params = query_params

        for k, v in self.endpoint_params.items():
            setattr(self, k, v)

        self.name = list(self.endpoint_params.keys())[0]
        self.value = list(self.endpoint_params.values())[0]

        for qp in self.query_params["params"]:
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
            result = self.template.format(self.value)
        else:
            result = self.value

        if hasattr(self, "force_url_encoding"):
            return quote_plus(result)

        return result

    @property
    def formatted_key(self):
        if hasattr(self, "url_key_string"):
            return self.url_key_string
        return self.name

    def __repr__(self):
        return f"EndpointParam {self.formatted_key}/{self.value}"

    def get_as_dict_url_formatted(self):
        if self.output_type == "arg":
            url_str = self.formatted_value
        if self.output_type == "kwarg":
            if getattr(self, "urlencode", False):
                url_str = urlencode({self.formatted_key: self.formatted_value})
            else:
                url_str = "{key}={value}".format(
                    key=self.formatted_key, value=self.formatted_value
                )
        return {self.name: url_str}

    def get_as_dict(self):
        return {self.name: self.get_formatted_value}
