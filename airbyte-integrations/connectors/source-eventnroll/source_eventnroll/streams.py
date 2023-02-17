from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional, List

import pendulum
import requests
from http import HTTPStatus
from airbyte_cdk.sources.streams.http import HttpStream

class EventnrollStream(HttpStream, ABC):

    url_base = "https://app.eventnroll.com/fr/api_main/"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.BATCH_INTERVAL = 1
        self.start = config['start']
        self.token = config['token']
        self.event_id = config['event_id']
        self.organizer_id = config['organizer_id']

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code not in [HTTPStatus.FORBIDDEN, HTTPStatus.OK]:
            response.raise_for_status()

        yield from response.json()

    def request_body_data(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:

        start = stream_slice[self.cursor_field]
        end = pendulum.parse(stream_slice[self.cursor_field]).add(days=self.BATCH_INTERVAL).to_datetime_string()

        return {
            "start": start,
            "end": end,
            "token": self.token,
            "event_id": self.event_id,
            "organizer_id": self.organizer_id,
        }


# Basic incremental stream
class IncrementalEventnrollStream(EventnrollStream, ABC):
    
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        return "date"

    def _chunk_date_range(self, start_date: pendulum.DateTime) -> List[Mapping[str, any]]:
        dates = []
        start_date = pendulum.parse(start_date)

        dates.append({self.cursor_field: start_date.to_datetime_string()})
        while start_date < pendulum.now():
            start_date = start_date.add(days=self.BATCH_INTERVAL)
            dates.append({self.cursor_field: start_date.to_datetime_string()})
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[
        Optional[Mapping[str, any]]]:
        start_date = stream_state[self.cursor_field] if stream_state and self.cursor_field in stream_state else self.start
        return self._chunk_date_range(start_date)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, ""), current_stream_state.get(self.cursor_field, ""))} 


class Orders(IncrementalEventnrollStream):

    http_method = "POST"
    primary_key = "order_line_ticket_id"

    def path(self, **kwargs) -> str:
        return "get_order_tickets_list"