import requests

from typing import Any, Mapping
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator


class BookerAuthenticator(HttpAuthenticator):

    def __init__(self, config):
        self.subscription_key = config["subscription_key"]
        self.access_token = self.login(config["url"], config["client_id"], config["client_secret"], config["grant_type"], config["scope"], config["subscription_key"])

    def login(self, url, client_id, client_secret, grant_type, scope, subscription_key):
        url = """{}v5/auth/connect/token""".format(url)
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type" : grant_type,
            "scope": scope
        
        }
        headers = { 
            'Content-Type': 'application/x-www-form-urlencoded',
            'Ocp-Apim-Subscription-Key': subscription_key
        }

        r = requests.post(url, data=data, headers=headers)
        r.raise_for_status()
        return r.json()['access_token']

    def get_auth_header(self) -> Mapping[str, Any]:
        return {
            'Content-Type': 'application/json',
            "Ocp-Apim-Subscription-Key": self.subscription_key
        }

    def get_access_token(self) -> str:
        return self.access_token