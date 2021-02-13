# -*- coding: utf-8 -*-

import requests

from requests.packages.urllib3.poolmanager import PoolManager
import ssl

class Tls1HttpAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLSv1
        )

url = 'https://mario.testkontur.ru/payments/import'

try:
    # data = json.dumps(data, encoding='utf-8')

    req = requests.Session()
    # req.cert = 'path to *.cer'
   # req.mount('https://', Tls1HttpAdapter())
    r = req.post(url, json=[], verify=True) #headers=self.headers
    if r.status_code == 200:
        a= 1
    print(r)
except Exception as exc:
    print(exc)