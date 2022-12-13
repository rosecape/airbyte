#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from .auth import LightspeedAuthenticator
from source_lightspeed.streams import (
    Categories,
    Customers,
    Discounts,
    Manufacturers,
    Items,
    ItemAttributeSets,
    ItemMatrices,
    PaymentTypes,
    Sales,
    SaleLines,
    SalePayments,
    Shops,
    TaxCategories,
    TaxClasses
)


class SourceLightspeed(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        LightspeedAuthenticator(config).authentication()
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token = LightspeedAuthenticator(config).authentication()
        config['authenticator'] = TokenAuthenticator(token=token)
        return [
            Categories(config),
            Customers(config),
            Discounts(config),
            Items(config),
            ItemAttributeSets(config),
            PaymentTypes(config),
            Manufacturers(config),
            Sales(config),
            SaleLines(config),
            SalePayments(config),
            Shops(config),
            TaxCategories(config),
            TaxClasses(config)
        ]
