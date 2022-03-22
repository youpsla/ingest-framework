# Inspired by:
# https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 30  # seconds


retry_strategy = Retry(
    total=7,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS", "POST"],
    backoff_factor=10,
)


def get_http_adapter():
    http = requests.Session()
    adapter = TimeoutHTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    return http


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)