from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from datetime import datetime, timedelta
import requests

from .auth import BookerAuthenticator
from airbyte_cdk.sources.streams.http import HttpStream

class BookerStream(HttpStream, ABC):
    
    def __init__(self, config: Mapping[str, Any], authenticator: BookerAuthenticator):
        super().__init__()
        self.config = config
        self.location_id = config["location_id"]
        self.access_token = authenticator.get_access_token()
        self.auth_header = authenticator.get_auth_header()

    @property
    def url_base(self) -> str:
        return f'{self.config["url"]}v4.1/customer/'
    
    @property
    def data_field(self) -> str:
        """The name of the field in the response which contains the data"""

    @property
    def cursor_field(self) -> str:
        """The name of the field in the response which contains the cursor"""

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        return 60

    def request_headers(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
        return self.auth_header

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,) -> Optional[Mapping]:
        data = {
            "LocationID": self.location_id,
            "access_token": self.access_token
        }
        return data

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json().get(self.data_field, []) if self.data_field is not None else response.json()

class IncrementalBookerStream(BookerStream, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator: BookerAuthenticator):
        super().__init__(config, authenticator)
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
        
class Treatments(BookerStream):
    
    def path(self, **kwargs) -> str:
        return "treatments"

    http_method = "POST"

    primary_key = "ID"
    data_field = "Treatments"


class Appointments(IncrementalBookerStream):

    def path(self, **kwargs) -> str:
        return "appointments"

    http_method = "POST"
    
    primary_key = "ID"
    data_field = "Results"
    cursor_field = "StartDateTimeOffset"

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,) -> Optional[Mapping]:
        print(self._cursor_value)
        super_data = super().request_body_json(stream_state, stream_slice, next_page_token)
        data = {
            "FromStartDateOffset": """{}T00:00:00-0400""".format(stream_slice[self.cursor_field]),
            "ToStartDateOffset": """{}T00:00:00-0400""".format((datetime.strptime(stream_slice[self.cursor_field], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')),
        }
        return {**super_data, **data}

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, any]]:
        dates = []
        while start_date < (datetime.now() - timedelta(days=1)):
            dates.append({self.cursor_field: start_date.strftime("%Y-%m-%d")})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], "%Y-%m-%d") if stream_state and self.cursor_field in stream_state else datetime.strptime(self.config["start_date"], "%Y-%m-%d")
        return self._chunk_date_range(start_date)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_date = datetime.strptime(record[self.cursor_field], '%Y-%m-%d')
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record