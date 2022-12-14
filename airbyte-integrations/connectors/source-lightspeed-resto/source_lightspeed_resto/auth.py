import requests

from typing import Any, Mapping


class LightspeedRestoAuthenticator():

    def __init__(self, config):
        self.config = config

    def authentication(self):
        url = "http://staging-integration.posios.com/PosServer/rest/token"

        data = {
            "companyId": self.config['companyId'],
            "deviceId": self.config['deviceId'],
            "password" : self.config['password'],
            "username": self.config['username'],
        }

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        r = requests.post(url, data=data, headers=headers)
        r.raise_for_status()

        return r.json()['token']