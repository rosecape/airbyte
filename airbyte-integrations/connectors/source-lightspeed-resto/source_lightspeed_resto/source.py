#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from .auth import LightspeedRestoAuthenticator

from source_lightspeed_resto.streams import (
    Customers,
    Products,
    ProductGroups,
    Receipts
)


class SourceLightspeedResto(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        LightspeedRestoAuthenticator(config).authentication()
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = LightspeedRestoAuthenticator(config=config)
        return [
            Customers(config, authenticator=auth),
            Products(config,  authenticator=auth),
            ProductGroups(config,  authenticator=auth),
            Receipts(config, authenticator=auth)
        ]