#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream

import dateutil.parser as parser


class LightspeedRestoStream(HttpStream, ABC):

    def __init__(self, config: Dict):
        super().__init__()
        self.config = config

    @property
    def url_base(self) -> str:
        return "https://staging-integration.posios.com/PosServer/rest/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response = response.json()
        offset = int(response.get('offset')) + int(response.get('limit'))
        if offset < int(response.json().get('count')):
            return offset
        return None

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        return {"Authorization": self.config['token']}

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

    order_field = "timeStamp"
    cursor_field = "timeStamp"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, ""), current_stream_state.get(self.cursor_field, ""))}

    def request_params(self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs):
        params = super().request_params(stream_state=stream_state,
                                        next_page_token=next_page_token, **kwargs) or {}
        params['orderby'] = self.cursor_field
        if not next_page_token:
            if stream_state:
                params[self.cursor_field] = f">,{parser.parse(stream_state.get(self.cursor_field))}"
        return params


class Customers(LightspeedRestoStream):
    """
    API docs: https://developers.lightspeedhq.com/resto-api/endpoints/corecustomer
    """
    primary_key = "id"
    
    def path(self, **kwargs) -> str:
        return "core/customer"