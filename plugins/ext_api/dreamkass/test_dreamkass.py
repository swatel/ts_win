# -*- coding: utf-8 -*-


import requests
import uuid

# url = 'https://kabinet.dreamkas.ru/api/oauth2/authorize?client_id=2&redirect_uri=http://127.0.0.1state=test'
# http://127.0.0.1state/=test?code=jIUAoXdlnnpX&state=


url = 'https://kabinet.dreamkas.ru/api/oauth2/access_token'

data = {
   "code": "jIUAoXdlnnpX",
   "client_id": 2,
   "client_secret": "aZq0A3oAPryG-wiT9vcXstNz"
}

# линкуем
r = requests.post(url, json=data, verify=False)
print(r.text)
# {"access_token":"-RqRheisfpcL_JRS0ki1ppg2"}
