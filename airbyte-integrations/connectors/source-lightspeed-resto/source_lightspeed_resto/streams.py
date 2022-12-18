#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, List

import requests
import dateutil.parser as parser
from datetime import datetime, timedelta, timezone

from airbyte_cdk.sources.streams.http import HttpStream
from source_lightspeed_resto.auth import LightspeedRestoAuthenticator


class LightspeedRestoStream(HttpStream, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator: LightspeedRestoAuthenticator):
        super().__init__()
        self.config = config

    @property
    def url_base(self) -> str:
        return "https://staging-integration.posios.com/PosServer/rest/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        offset = int(response.get('offset')) + int(response.get('amount'))
        if offset < int(response.get('total')):
            return offset
        return None

    def request_params(
        self, next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {}
        if next_page_token:
            params['offset'] = next_page_token
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json().get('results') if 'results' in response.json() else response.json()


class IncrementalLightspeedRestoStream(LightspeedRestoStream, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator: LightspeedRestoAuthenticator):
        super().__init__(config, authenticator)
        self._cursor_value = None

    # order_field = "timeStamp"
    cursor_field = "modificationDate"
    
    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.config['start_date']}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
       self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d')

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, any]]:
        dates = []
        while start_date < (datetime.now() - timedelta(days=1)):
            dates.append({self.cursor_field: start_date.strftime("%Y-%m-%d")})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], "%Y-%m-%d") if stream_state and self.cursor_field in stream_state else datetime.strptime(self.config["start_date"], "%Y-%m-%d")
        return self._chunk_date_range(start_date)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, ""), current_stream_state.get(self.cursor_field, ""))}

    def request_params(self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs):
        params = super().request_params(stream_state=stream_state,
                                        next_page_token=next_page_token, **kwargs) or {}
        params['useModification'] = "true"
        params['orderby'] = self.cursor_field
        if not next_page_token:
            if stream_state:
                params[self.cursor_field] = f">,{parser.parse(stream_state.get(self.cursor_field))}"
        return params

    def read_records(self, stream_state: Mapping[str, Any] = None, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(stream_slice=stream_slice, **kwargs)
        if next(records, object()) is not object():
            for record in super().read_records(stream_slice=stream_slice, **kwargs):
                cursor_value = self._cursor_value.replace(tzinfo=timezone.utc)
                latest_record_date = datetime.strptime(record[self.cursor_field], '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=timezone.utc)
                self._cursor_value = max(cursor_value, latest_record_date) if cursor_value else latest_record_date
                yield record
        self._cursor_value = datetime.strptime(stream_slice[self.cursor_field], '%Y-%m-%d').replace(tzinfo=timezone.utc)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        offset = int(response.get('offset')) + int(response.get('amount'))
        if offset < int(response.get('total')):
            return offset
        return None


class Customers(LightspeedRestoStream):
    """
    API docs: https://developers.lightspeedhq.com/resto-api/endpoints/corecustomer
    """
    primary_key = "id"
    
    def path(self, **kwargs) -> str:
        return "core/customer"

class Products(LightspeedRestoStream):
    """
    API docs: https://developers.lightspeedhq.com/resto-api/endpoints/inventoryproduct
    """
    primary_key = "id"
    
    def path(self, **kwargs) -> str:
        return "inventory/product"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

class Receipts(IncrementalLightspeedRestoStream):
    """
    API docs: https://developers.lightspeedhq.com/resto-api/endpoints/financial/#get-all-receipts-for-a-certain-date-or-date-range
    """
    primary_key = "id"
    
    def path(self, **kwargs) -> str:
        return "financial/receipt"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None