from typing import Any, Dict, Mapping

import requests
from airbyte_cdk import AirbyteLogger


class LightspeedAuthenticator():

    """
    Authentication helper for Lightspeed API
    """

    def __init__(self, config: Mapping[str, Any]):
        self.config = config

    def get_auth_payload(self) -> Mapping[str, Any]:
        return {
            "client_id": self.config['client_id'],
            "client_secret": self.config['client_secret'],
            "refresh_token": self.config['refresh_token'],
            "grant_type": 'refresh_token',
        }

    def authentication(self) -> Mapping[str, Any]:

        base_url = "https://api.lightspeedapp.com/"
        url = "{}/oauth/access_token.php".format(base_url)

        try:
            payload = self.get_auth_payload()
            response = requests.request("POST", url, data=payload)
            token = response.json().get('access_token')
            if not token:
                raise Exception('An error occured while generating token')
            print('Lightspeed token successfully generated')
            return token
        except:
            raise Exception('An error occured while generating token')
