from urllib.parse import urlencode


class EndPoint:
    def __init__(self, params):
        self.params = params
        self.category = params["category"]

    def set_attrs(self):
        for p in self.params:
            setattr(self, p, self.params[p])

    def build_endpoint(self):
        print(urlencode(params))
        endpoint = (
            "https://api.linkedin.com/v2/"
            f"{category if category else ''}?"
            f"{'q='+q if q else ''}"
            f"{'&' + urlencode(params) if params else ''}"
        )

        return endpoint

    def build_url_params(self):
        pass
