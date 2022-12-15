import requests

from typing import Any, Mapping


class LightspeedRestoAuthenticator():

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