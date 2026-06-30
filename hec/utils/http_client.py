import warnings
from typing import Any, Mapping, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry


class HttpClient:
    """Small requests.Session wrapper with shared timeout, retry, and TLS defaults."""

    def __init__(
            self,
            default_timeout_seconds: float = 10.0,
            retries: int = 2,
            backoff_factor: float = 0.2,
            verify_tls: bool = True,
            session: Optional[requests.Session] = None,
    ):
        self.default_timeout_seconds = default_timeout_seconds
        self.verify_tls = verify_tls
        self.session = session or requests.Session()

        retry_policy = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS", "PUT", "POST", "DELETE"}),
        )
        adapter = HTTPAdapter(max_retries=retry_policy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def request(self, method: str, url: str, **kwargs: Any):
        kwargs.setdefault("timeout", self.default_timeout_seconds)
        kwargs.setdefault("verify", self.verify_tls)
        if kwargs.get("verify") is False:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", InsecureRequestWarning)
                return self.session.request(method.upper(), url, **kwargs)
        return self.session.request(method.upper(), url, **kwargs)

    def get(self, url: str, **kwargs: Any):
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any):
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any):
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any):
        return self.request("DELETE", url, **kwargs)


def build_http_client(app_config: Optional[Mapping[str, Any]] = None, **overrides: Any) -> HttpClient:
    config = dict((app_config or {}).get("http", {}))
    config.update({key: value for key, value in overrides.items() if value is not None})
    return HttpClient(
        default_timeout_seconds=float(config.get("default_timeout_seconds", 10.0)),
        retries=int(config.get("retries", 2)),
        backoff_factor=float(config.get("backoff_factor", 0.2)),
        verify_tls=bool(config.get("verify_tls", True)),
    )
