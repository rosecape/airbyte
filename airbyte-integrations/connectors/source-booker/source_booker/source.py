#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime, timedelta


import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator

from auth import BookerAuthenticator


class BookerStream(HttpStream, ABC):
    url_base = None
    
    def __init__(self, config: Mapping[str, Any], authenticator: BookerAuthenticator):
        super().__init__()
        self.url_base = """{}v4.1/customer/""".format(config["url"])
        self.location_id = config["location_id"]
        self.access_token = authenticator.get_access_token()
        self.auth_header = authenticator.get_auth_header()

    def request_headers(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
        return self.auth_header

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        return {}

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,) -> Optional[Mapping]:
        data = {
            "LocationID": self.location_id,
            "access_token": self.access_token
        }
        return data

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


class Treatments(BookerStream):
    primary_key = "ID"

    @property
    def http_method(self) -> str:
        return "POST"

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return "treatments"

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Iterable[Mapping]:
        return response.json()["Treatments"]


class Appointments(BookerStream):
    
    cursor_field = "StartDateTimeOffset"
    primary_key = "ID"

    def __init__(self, config: Mapping[str, Any], authenticator: BookerAuthenticator):
        super().__init__(config, authenticator)
        self.start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.start_date.strftime('%Y-%m-%d')}

    @state.setter
    def state(self, value: Mapping[str, Any]):
       self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d')

    @property
    def http_method(self) -> str:
        return "POST"

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        return 60

    def path(self, **kwargs) -> str:
        return "appointments"

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,) -> Optional[Mapping]:
        super_data = super().request_body_json(stream_state, stream_slice, next_page_token)
        data = {
            "FromStartDateOffset": """{}T00:00:00-0400""".format(stream_slice[self.cursor_field]),
            "ToStartDateOffset": """{}T00:00:00-0400""".format((datetime.strptime(self.state[self.cursor_field], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')),
        }
        return {**super_data, **data}

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, any]]:
        dates = []
        while start_date < (datetime.now() - timedelta(days=1)):
            dates.append({self.cursor_field: start_date.strftime("%Y-%m-%d")})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], "%Y-%m-%d") if stream_state and self.cursor_field in stream_state else self.start_date
        return self._chunk_date_range(start_date)

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Iterable[Mapping]:
        response.raise_for_status()
        self._cursor_value = datetime.strptime(self.state[self.cursor_field], '%Y-%m-%d') + timedelta(days=1)
        return response.json()["Results"]


# Source
class SourceBooker(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        data = {
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "grant_type" : config["grant_type"],
            "scope": config["scope"]
        }
        headers = { 
            'Content-Type': 'application/x-www-form-urlencoded',
            'Ocp-Apim-Subscription-Key': config["subscription_key"]
        }
        r = requests.post("""{}v5/auth/connect/token""".format(config["url"]), data=data, headers=headers)
        r.raise_for_status()
        return True, None
        
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BookerAuthenticator(config=config)
        return [
            Treatments(config=config, authenticator=auth),
            Appointments(config=config, authenticator=auth)
        ]


