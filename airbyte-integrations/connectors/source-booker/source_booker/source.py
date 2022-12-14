#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from auth import BookerAuthenticator

from source_booker.streams import (
    Treatments,
    Appointments
)


class SourceBooker(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        data = {
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "grant_type" : config["grant_type"],
            "scope": config["scope"]
        }
        headers = { 
            'Content-Type': 'application/x-www-form-urlencoded',
            'Ocp-Apim-Subscription-Key': config["subscription_key"]
        }
        r = requests.post("""{}v5/auth/connect/token""".format(config["url"]), data=data, headers=headers)
        r.raise_for_status()
        return True, None
        
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BookerAuthenticator(config=config)
        return [
            Treatments(config=config, authenticator=auth),
            Appointments(config=config, authenticator=auth)
        ]


