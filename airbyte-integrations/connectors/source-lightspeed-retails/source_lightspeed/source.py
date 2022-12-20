#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator

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
        authenticator = Oauth2Authenticator(
            token_refresh_endpoint="https://api.lightspeedapp.com/oauth/access_token.php",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"]
        )
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = Oauth2Authenticator(
            token_refresh_endpoint="https://api.lightspeedapp.com/oauth/access_token.php",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"]
        )
        return [
            Categories(config, authenticator),
            Customers(config , authenticator),
            Discounts(config, authenticator),
            Items(config, authenticator),
            ItemAttributeSets(config, authenticator),
            PaymentTypes(config, authenticator),
            Manufacturers(config, authenticator),
            Sales(config, authenticator),
            SaleLines(config, authenticator),
            SalePayments(config, authenticator),
            Shops(config, authenticator),
            TaxCategories(config, authenticator),
            TaxClasses(config, authenticator),
        ]
