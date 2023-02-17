#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from .streams import Orders

class SourceEventnroll(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            # stream = Orders(config=config)
            # records = stream.read_records(sync_mode=SyncMode.incremental)
            # next(records)
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

        

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            Orders(config),
        ]