#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .auth import BookerAuthenticator

from source_booker.streams import (
    Treatments,
    Appointments
)


class SourceBooker(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        auth = BookerAuthenticator(config=config)
        return True, None
        
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BookerAuthenticator(config=config)
        return [
            Treatments(config=config, authenticator=auth),
            Appointments(config=config, authenticator=auth)
        ]


