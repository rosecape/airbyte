#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, List

import requests
import pendulum
from pendulum import DateTime, Period

from urllib.parse import urlparse
from urllib.parse import parse_qs

import dateutil.parser as parser
from datetime import datetime, timedelta, timezone

from airbyte_cdk.models import SyncMode

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
        
        response_json = response.json()
        results = response_json.get('results') if 'results' in response_json else response_json

        # Fast return if no results
        if not results:
            return None

        # Default strategy for paginated endpoints
        if 'offset' and 'amount' and 'total' in response:
            offset = int(response_json.get('offset')) + int(response_json.get('amount'))
            if offset < int(response_json.get('total')):
                return offset

        # Custom strategy for paginated endpoints not returning pagination metadata
        else:
            parsed = parse_qs(urlparse(response.url).query)
            captured_value = parsed['offset'][0] if 'offset' in parsed else None
            
            if captured_value:
                return len(results) + int(captured_value)
            return len(results)

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
    
    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: self.config['start_date']}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        return self.state

    def chunk_date_range(self, start_date: DateTime, interval=pendulum.duration(days=1), end_date: Optional[DateTime] = None) -> Iterable[Period]:
        """
        Yields a list of the beginning and ending timestamps of each day between the start date and now.
        The return value is a pendulum.period
        """        
        end_date = pendulum.now()
        # Each stream_slice contains the beginning and ending timestamp for a 24 hour period
        chunk_start_date = start_date

        while chunk_start_date < end_date:
            chunk_end_date = min(chunk_start_date + interval, end_date)
            yield pendulum.period(chunk_start_date, chunk_end_date)
            chunk_start_date = chunk_end_date
        
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        dates = []
        date_format = 'YYYY-MM-DDTHH:mm:ss.SSS'
        date_value = self._cursor_value if self._cursor_value else self.config['start_date']
        start_date = pendulum.parse(date_value)
        for period in self.chunk_date_range(start_date=start_date):
            dates.append({"oldest": period.start.format(date_format), "latest": period.end.format(date_format)})
        return dates

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        
        records = super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        for record in records:
            next_cursor_value = record[self.cursor_field]
            if sync_mode == SyncMode.incremental:
                self._cursor_value = max(self._cursor_value, next_cursor_value) if self._cursor_value else next_cursor_value
            yield record

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page = {}
        previous_query = parse_qs(urlparse(response.request.url).query)
        
        if len(response.json().get('results')) == 0:
            '''At this point, the cursor value is set to the last record read.
                However, if there are no more records for the given day, it means 
                the day as been fully read and should not be read again. We therefore
                set the value of the cursor to the previous "to" to be taken as the next
                "from" value.'''
            self._cursor_value = previous_query["to"][0]
            return None

        next_page['offset'] = int(response.json().get('offset')) + int(response.json().get('amount'))
        next_page['previous_to'] = previous_query["to"][0]
        next_page['previous_from'] = previous_query["from"][0]
        
        return next_page


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

class ProductGroups(LightspeedRestoStream):
    """
    API docs: https://developers.lightspeedhq.com/resto-api/endpoints/inventoryproductgroups
    """
    primary_key = "id"
    
    def path(self, **kwargs) -> str:
        return "inventory/productgroup"

class Receipts(IncrementalLightspeedRestoStream):
    """
    API docs: https://developers.lightspeedhq.com/resto-api/endpoints/financial/#get-all-receipts-for-a-certain-date-or-date-range
    """
    primary_key = "id"
    cursor_field = "modificationDate"
    order_field = "modificationDate"
    
    def path(self, **kwargs) -> str:
        return "financial/receipt"

    def request_params(self, stream_slice, next_page_token: Mapping[str, Any], **kwargs):
        params = {}
        
        # Basic request parameters
        params['useModification'] = "true"
        params['orderby'] = self.cursor_field

        if next_page_token is None:

            params["from"] = stream_slice["oldest"]
            params["to"] = stream_slice["latest"]

        else:
            # 3. If the stream is not new but we have to iterate over the same date range
            if 'offset' in next_page_token:
                params["offset"] = next_page_token['offset']
                params["from"] = next_page_token['previous_from']
                params["to"] = next_page_token['previous_to']

        return params