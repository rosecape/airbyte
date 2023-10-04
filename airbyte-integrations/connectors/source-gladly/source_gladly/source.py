#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod, abstractproperty
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import re
import csv
import requests
import pendulum
from io import StringIO

from pendulum import DateTime, Period

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import BasicHttpAuthenticator


# Basic full refresh stream
class GladlyStream(HttpStream, ABC):

    def snakecase_keys(self, data):
        if isinstance(data, dict):
            return {self.snakecase_header(key): self.snakecase_keys(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.snakecase_keys(item) for item in data]
        else:
            return data

    def snakecase_header(self, header):
        # Custom snakecasing function for headers
        header = header.lower().replace(" ", "_").replace('%', 'percentage')  # Replace spaces with underscores
        header = re.sub(r"[^a-zA-Z0-9_]", "", header)  # Remove special characters
        return header

    # TODO: Fill in the url base. Required.
    url_base = "https://groupenordik.us-1.gladly.com/api/v1/"

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """This method is called if we run into the rate limit.
        Gladly does not put the retry time in the `Retry-After` response header so we
        we return a default value. If the response is anything other than a 429 (e.g: 5XX)
        fall back on default retry behavior."""

        if "Retry-After" in response.headers:
            return int(response.headers["Retry-After"])
        else:
            self.logger.info("Retry-after header not found. Using default backoff value")
            return 5

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:

        # Handle 'Ratelimit-Remaining-Second' header
        if 'Ratelimit-Remaining-Second' in response.headers:
            ratelimit_remaining_second = int(response.headers['Ratelimit-Remaining-Second'])
            if ratelimit_remaining_second == 0:
                self.logger.info("Ratelimit-Remaining-Second is 0. backoff will be triggered")
                super().backoff_time(response)
        
        if 'csv' in response.headers.get('content-type', ''):
            # Parse CSV response
            csv_data = response.text
            csv_reader = csv.DictReader(StringIO(csv_data))

            for row in csv_reader:
                # Snakecase the keys in each row
                snakecased_row = {self.snakecase_header(key): value for key, value in row.items()}
                yield snakecased_row
        else:
            # if list, iterate over each item and snakecase the keys
            if isinstance(response.json(), list):
                for item in response.json():
                    yield item
            else:
                yield response.json()

# Incremental Streams
def chunk_date_range(start_date: DateTime, interval=pendulum.duration(days=1), end_date: Optional[DateTime] = None) -> Iterable[Period]:
    """
    Yields a list of the beginning and ending timestamps of each day between the start date and now.
    The return value is a pendulum.period
    """

    end_date = end_date or pendulum.now()
    # Each stream_slice contains the beginning and ending timestamp for a 24 hour period
    chunk_start_date = start_date

    while chunk_start_date < end_date:
        chunk_end_date = min(chunk_start_date + interval, end_date)
        yield pendulum.period(chunk_start_date, chunk_end_date)
        chunk_start_date = chunk_end_date


class Organization(GladlyStream):

    primary_key = "id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "organization"


# Basic incremental stream
class IncrementalGladlyStream(GladlyStream, ABC):

    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.config = config

    state_checkpoint_interval = None

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {self.cursor_field: self._cursor_value} if self._cursor_value else {}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field, self.config['start_date'])
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """Break up the stream into time slices. Each slice is a dictionary of start and end timestamps.
        The start timestamp is the last time the stream was run. The end timestamp is the current time.
        The stream will be run once for each slice.
        """
        start_date = pendulum.parse(stream_state.get(self.cursor_field) if stream_state else self.config['start_date'])
        end_date = pendulum.now() # Shoudl stop at now - 1 day to prevent duplicates from append

        for chunk in chunk_date_range(start_date=start_date, end_date=end_date):
            yield {"start_date": chunk.start, "end_date": chunk.end}

    def read_records(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Mapping[str, Any]]:
        yield from super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)
        print(stream_slice)
        if "end_date" in stream_slice:
            self._cursor_value = stream_slice["end_date"].format('YYYY-MM-DD')

class IncrementalGladlyReportStream(IncrementalGladlyStream, ABC):
    """ Special class for Gladly reports as they require a POST request with a JSON body
    that is different for each report. Non report streams can use the GET request and their
    structure is more consistent."""
    
    http_method = "POST"
    
    @abstractproperty
    def metric_set(self):
        pass
    
    @abstractproperty
    def aggregation_level(self):
        pass

    def path(self, **kwargs) -> str:
        return "reports"

    def request_body_json(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> Mapping[str, Any]:
        return {
            "metricSet": self.metric_set,
            "aggregationLevel": self.aggregation_level,
            "startAt": stream_slice["start_date"].format('YYYY-MM-DD'),
            "endAt": stream_slice["end_date"].format('YYYY-MM-DD')
        }
    
class AgentAwayTimeReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "agentAwayTimeReport"
    aggregation_level = "halfHourly"
    
class AgentDurationsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "agentDurationsReport"
    aggregation_level = None
    
class AgentSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "agentSummary"
    aggregation_level = None
    
class AgentSummaryV2(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "agentSummaryV2"
    aggregation_level = None

class AgentTimestampsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "agentTimestampsReport"
    aggregation_level = None
    
class AnswerUsageByAgentReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "answerUsageByAgentReport"
    aggregation_level = None
    
class AnswerUsageReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "answerUsageReport"
    aggregation_level = None
    
class AutoThrottleChangesReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "autoThrottleChangesReport"
    aggregation_level = None
    
class AutoThrottleMissedConversationsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "autoThrottleMissedConversationsReport"
    aggregation_level = None
    
class ChannelMixReportV2(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "channelMixReportV2"
    aggregation_level = None
    
class ChannelWaitTimeReportV2(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "channelWaitTimeReportV2"
    aggregation_level = None
    
class ChatDisplayPctChangesReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "chatDisplayPctChangesReport"
    aggregation_level = None
    
class ContactExportReportV2(IncrementalGladlyReportStream):

    primary_key = "contact_id"

    metric_set = "contactExportReportV2"
    aggregation_level = None
    
class ContactExportReportV3(IncrementalGladlyReportStream):

    @property
    def cursor_field(self):
        return "queued_at"

    primary_key = "contact_id"
    metric_set = "contactExportReportV3"
    aggregation_level = None
    
class ContactExportReport(IncrementalGladlyReportStream):

    primary_key = "contact_id"
    metric_set = "contactExportReport"
    aggregation_level = None
    
class ChannelWaitTimeReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "channelWaitTimeReport"
    aggregation_level = None
    
class ContactSummaryCountsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryCountsReport"
    aggregation_level = None
    
class ContactSummaryDurationsReportV2(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryDurationsReportV2"
    aggregation_level = None
    
class ContactSummaryDurationsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryDurationsReport"
    aggregation_level = None
    
class ContactSummaryReportV2(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryReportV2"
    aggregation_level = None
    
class ContactSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryReport"
    aggregation_level = None

class ContactTimestampsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactTimestampsReport"
    aggregation_level = None

class ConversationExportReport(IncrementalGladlyReportStream):

    @property
    def use_cache(self) -> bool:
        return True
    
    @property
    def cursor_field(self):
        return "created_at"

    primary_key = "conversation_id"

    metric_set = "conversationExportReport"
    aggregation_level = None

class ConversationSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "conversationSummaryReport"
    aggregation_level = None
    
class ConversationTimestampsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "conversationTimestampsReport"
    aggregation_level = None
    
class FirstContactResolutionByAgentV2Report(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "firstContactResolutionByAgentV2Report"
    aggregation_level = None
    
class HelpCenterAnswerSearchReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "helpCenterAnswerSearchReport"
    aggregation_level = None
    
class QuickActionsUsageReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "quickActionsUsageReport"
    aggregation_level = None

class SidekickAnswerUsageReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "sidekickAnswerUsageReport"
    aggregation_level = None

class TaskSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "taskSummaryReport"
    aggregation_level = None

class AgentLoginTimeReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "agentLoginTimeReport"
    aggregation_level = "halfHourly"
    
class AgentSummaryGlanceReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "agentSummaryGlanceReport"
    aggregation_level = "halfHourly"
    
class FirstContactResolutionByAgentV2Report(IncrementalGladlyReportStream):
    
    primary_key = "id"
    metric_set = "firstContactResolutionByAgentV2Report"
    aggregation_level = "halfHourly"
        
class AnswerUsageReport(IncrementalGladlyReportStream):
        
    primary_key = "id"
    metric_set = "answerUsageReport"
    aggregation_level = "halfHourly"

class AnswerUsageByAgentReport(IncrementalGladlyReportStream):
            
    primary_key = "id"
    metric_set = "answerUsageByAgentReport"
    aggregation_level = "halfHourly"
                
class ChannelMixReportV2(IncrementalGladlyReportStream):
                    
    primary_key = "id"
    metric_set = "channelMixReportV2"
    aggregation_level = "halfHourly"

class ChannelWaitTimeReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "channelWaitTimeReport"
    aggregation_level = "halfHourly"
    
class ChannelWaitTimeReportV2(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "channelWaitTimeReportV2"
    aggregation_level = "halfHourly"
    
class ContactSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryReport"
    aggregation_level = "halfHourly"
    
class ContactSummaryReportV2(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryReportV2"
    aggregation_level = "halfHourly"
    
class ContactSummaryCountsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryCountsReport"
    aggregation_level = "halfHourly"
    
class ContactSummaryDurationsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "contactSummaryDurationsReport"
    aggregation_level = "halfHourly"
    
class ConversationSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "conversationSummaryReport"
    aggregation_level = "halfHourly"
    
class AbandonedCallsInIVRReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "abandonedCallsInIVRReport"
    aggregation_level = "halfHourly"
    
class AutoThrottleMissedConversationsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "autoThrottleMissedConversationsReport"
    aggregation_level = "halfHourly"
    
class PaymentsSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "paymentsSummaryReport"
    aggregation_level = "halfHourly"

class ProactiveVoiceSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "proactiveVoiceSummaryReport"
    aggregation_level = "halfHourly"
    
class TopicHierarchyReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "topicHierarchyReport"
    aggregation_level = "halfHourly"
    
class TaskSummaryReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "taskSummaryReport"
    aggregation_level = "halfHourly"
    

class HelpCenterAnswerUsageReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "helpCenterAnswerUsageReport"
    aggregation_level = "halfHourly"
    
# class IvrEndStatesReportV2(IncrementalGladlyReportStream):

#     primary_key = "id"
#     metric_set = "ivrEndStatesReportV2"
#     aggregation_level = None
    
# class IvrExecutiveSummaryReportV2(IncrementalGladlyReportStream):

#     primary_key = "id"
#     metric_set = "ivrExecutiveSummaryReportV2"
#     aggregation_level = None
    
class PaymentsByAgentReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "paymentsByAgentReport"
    aggregation_level = None
    
class QuickActionsUsageReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "quickActionsUsageReport"
    aggregation_level = "halfHourly"
    
class SidekickAnswerUsageReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "sidekickAnswerUsageReport"
    aggregation_level = None

class SidekickAnswerSearchReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "sidekickAnswerSearchReport"
    aggregation_level = "halfHourly"
    
class SidekickContactPointsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "sidekickContactPointsReport"
    aggregation_level = None

class TaskExportReport(IncrementalGladlyReportStream):

    primary_key = "task_id"
    metric_set = "taskExportReport"
    aggregation_level = None
    
class TaskTimestampsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "taskTimestampsReport"
    aggregation_level = None
    
class WorkSessionEventsReportV3(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "workSessionEventsReportV3"
    aggregation_level = None

class WorkSessionsReportV3(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "workSessionsReportV3"
    aggregation_level = None
    
class WorkSessionsReport(IncrementalGladlyReportStream):

    primary_key = "id"
    metric_set = "workSessionsReport"
    aggregation_level = None
    
class GladlySubStream(IncrementalGladlyStream, HttpSubStream):

    @property
    @abstractmethod
    def path_template(self) -> str:
        """
        :return: sub stream path template
        """

    @property
    @abstractmethod
    def parent(self) -> IncrementalGladlyStream:
        """
        :return: parent stream class
        """

    @property
    @abstractmethod
    def foreign_key(self) -> IncrementalGladlyStream:
        """
        :return: foreign key
        """

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return self.path_template.format(customer_id=stream_slice["parent_id"])
    
    def stream_slices(
            self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
        ) -> Iterable[Optional[Mapping[str, Any]]]:
            for parent_slice in self.parent.stream_slices(sync_mode=SyncMode.incremental, cursor_field=cursor_field, stream_state=stream_state):
                for record in self.parent.read_records(sync_mode=SyncMode.incremental, cursor_field=cursor_field, stream_slice=parent_slice, stream_state=stream_state):
                    yield {"parent_id": record[self.foreign_key]}

class Agents(GladlyStream):
    """
    Docs: https://developer.gladly.com/rest/#tag/Agents
    
    """
        
    http_method = "GET"
    
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "agents"

class Customers(GladlySubStream):
    """
    Docs: https://developer.gladly.com/rest/#tag/Customers
    
    """
        
    http_method = "GET"
    
    primary_key = "id"
    cursor_field = "created_at"
    foreign_key = "customer_id"
    
    parent = ConversationExportReport
    path_template = "customer-profiles/{customer_id}"


# Source
class SourceGladly(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = BasicHttpAuthenticator(username=config['username'], password=config['api_token'])
            records = Organization(authenticator=auth).read_records(sync_mode=None)
            next(records)
            return True, None
        except Exception as error:
            return False, f"Unable to connect to Gladly API with the provided credentials - {error}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BasicHttpAuthenticator(username=config['username'], password=config['api_token'])
        return [
            # AbandonedCallsInIVRReport(authenticator=auth, config=config),
            Agents(authenticator=auth),
            # AgentAwayTimeReport(authenticator=auth, config=config),
            # AgentDurationsReport(authenticator=auth, config=config),
            # AgentLoginTimeReport(authenticator=auth, config=config),
            # AgentSummaryGlanceReport(authenticator=auth, config=config),
            # AgentSummaryReport(authenticator=auth, config=config),
            # AgentSummaryV2(authenticator=auth, config=config),
            # AgentTimestampsReport(authenticator=auth, config=config),
            # AnswerUsageByAgentReport(authenticator=auth, config=config),
            # AnswerUsageReport(authenticator=auth, config=config),
            # AutoThrottleChangesReport(authenticator=auth, config=config),
            # AutoThrottleMissedConversationsReport(authenticator=auth, config=config),
            # ChannelMixReportV2(authenticator=auth, config=config),
            # ChannelWaitTimeReport(authenticator=auth, config=config),
            # ChannelWaitTimeReportV2(authenticator=auth, config=config),
            # ChatDisplayPctChangesReport(authenticator=auth, config=config),
            # ContactExportReportV2(authenticator=auth, config=config),
            ContactExportReportV3(authenticator=auth, config=config),
            # ContactExportReport(authenticator=auth, config=config),
            # ContactSummaryCountsReport(authenticator=auth, config=config),
            # ContactSummaryDurationsReportV2(authenticator=auth, config=config),
            # ContactSummaryDurationsReport(authenticator=auth, config=config),
            # ContactSummaryReport(authenticator=auth, config=config),
            # ContactSummaryReportV2(authenticator=auth, config=config),
            # ContactTimestampsReport(authenticator=auth, config=config),
            ConversationExportReport(authenticator=auth, config=config),
            # ConversationSummaryReport(authenticator=auth, config=config),
            # ConversationTimestampsReport(authenticator=auth, config=config),
            Customers(authenticator=auth, config=config, parent=ConversationExportReport(authenticator=auth, config=config)),
            # FirstContactResolutionByAgentV2Report(authenticator=auth, config=config),
            # HelpCenterAnswerSearchReport(authenticator=auth, config=config),
            # HelpCenterAnswerUsageReport(authenticator=auth, config=config),
            # IvrEndStatesReportV2(authenticator=auth, config=config),
            # IvrExecutiveSummaryReportV2(authenticator=auth, config=config),
            # PaymentsByAgentReport(authenticator=auth, config=config),
            # PaymentsSummaryReport(authenticator=auth, config=config),
            # ProactiveVoiceSummaryReport(authenticator=auth, config=config),
            # QuickActionsUsageReport(authenticator=auth, config=config),
            # SidekickAnswerSearchReport(authenticator=auth, config=config),
            # SidekickAnswerUsageReport(authenticator=auth, config=config),
            # SidekickContactPointsReport(authenticator=auth, config=config),
            # TaskExportReport(authenticator=auth, config=config),
            # TaskSummaryReport(authenticator=auth, config=config),
            # TaskTimestampsReport(authenticator=auth, config=config),
            # TopicHierarchyReport(authenticator=auth, config=config),
            # WorkSessionEventsReportV3(authenticator=auth, config=config),
            # WorkSessionsReportV3(authenticator=auth, config=config),
            # WorkSessionsReport(authenticator=auth, config=config),
        ]
