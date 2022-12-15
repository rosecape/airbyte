#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from .auth import LightspeedRestoAuthenticator

from source_lightspeed_resto.streams import (
    Customers
)


class SourceLightspeedResto(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        LightspeedRestoAuthenticator(config).authentication()
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token = LightspeedRestoAuthenticator(config).authentication()
        config['token'] = token
        return [
            Customers(config)
        ]