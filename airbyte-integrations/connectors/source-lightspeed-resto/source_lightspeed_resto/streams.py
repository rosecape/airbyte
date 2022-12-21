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

from airbyte_cdk.sources.streams import IncrementalMixin


class LightspeedRestoStream(HttpStream, ABC):

    def __init__(self, config: Mapping[str, Any], authenticator: LightspeedRestoAuthenticator):
        super().__init__()
        self.config = config
        self._authenticator = authenticator

    @property
    def url_base(self) -> str:
        return f"{self.config['url']}/" if self.config['production'] else "https://staging-integration.posios.com/PosServer/rest/"

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        return self._authenticator.get_auth_header()

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


class IncrementalLightspeedRestoStream(LightspeedRestoStream, IncrementalMixin):

    def __init__(self, config: Mapping[str, Any], authenticator: LightspeedRestoAuthenticator):
        super().__init__(config, authenticator)
        self._cursor_value = None

    order_field = "modificationDate"
    cursor_field = "modificationDate"
    
    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
        else:
            return {self.cursor_field: self.config['start_date']}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, any]]:
        dates = []
        while start_date < (datetime.now() - timedelta(days=1)):
            dates.append({self.cursor_field: start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")})
            start_date += timedelta(days=1)
        return dates
        
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_date = stream_state[self.cursor_field] if stream_state and self.cursor_field in stream_state else self.config["start_date"]
        return self._chunk_date_range(datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S.%fZ'))

    def read_records(self, stream_state: Mapping[str, Any] = None, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(stream_slice=stream_slice, **kwargs)
        if next(records, object()) is not object():
            for record in records:
                cursor_value = self._cursor_value
                latest_record_date = record[self.cursor_field]
                self._cursor_value = max(cursor_value, latest_record_date) if cursor_value else latest_record_date
                yield record
        self._cursor_value = stream_slice[self.cursor_field]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        offset = int(response.get('offset')) + int(response.get('amount'))
        if 'total' in response and offset < int(response.get('total')):
            return offset
        return None


class Customers(LightspeedRestoStream):
    """
    API docs: https://developers.lightspeedhq.com/resto-api/endpoints/corecustomer
    """
    primary_key = "id"
    cursor_field = "modifiedSince"
    
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

    def request_params(self, stream_slice, stream_state: Mapping[str, Any], next_page_token: Mapping[str, Any], **kwargs):
        params = super().request_params(stream_state=stream_state,
                                        next_page_token=next_page_token, **kwargs) or {}
        params['useModification'] = "true"
        params['orderby'] = self.cursor_field

        if not next_page_token:
            params['from'] = self._cursor_value or stream_slice[self.cursor_field]
            params['to'] = (datetime.strptime(params['from'], '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        return params