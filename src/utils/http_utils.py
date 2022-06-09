# Inspired by:
# https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, ConnectTimeout, RetryError
from requests.packages.urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 10  # seconds


# Define the retry strategy.
# Details: https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS", "POST"],
    backoff_factor=5,
)


def get_http_adapter():
    http = requests.Session()
    adapter = TimeoutHTTPAdapter(
        max_retries=retry_strategy, pool_connections=40, pool_maxsize=40
    )
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    return http


class TimeoutHTTPAdapter(HTTPAdapter):
    """Used to set default timeout for all requests."""

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
