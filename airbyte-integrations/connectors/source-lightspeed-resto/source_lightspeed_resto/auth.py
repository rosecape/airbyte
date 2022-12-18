import requests

from typing import Any, Mapping

from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

class LightspeedRestoAuthenticator(TokenAuthenticator):

    def __init__(self, config):
        self.config = config

    def authentication(self):
        url = "https://staging-integration.posios.com/PosServer/rest/token"

        data = {
            "companyId": self.config['companyId'],
            "deviceId": self.config['deviceId'],
            "password" : self.config['password'],
            "username": self.config['username']
        }

        headers = {'Content-Type': 'application/json', 'Accept': '*/*', 'Content-Length': '126'}

        response = requests.request("POST", url, json=data, headers=headers)
        token = response.json().get('token')
        if not token:
            raise Exception('An error occured while generating token')
        return token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Authorization": self.authentication()}