# -*- coding: utf-8 -*-


import requests
import uuid

url = 'https://cloud.mypos.ru/api/'

url_link = 'sync/link'
# syncId = uuid.uuid1()
syncId = 'f6bcf19e-205b-11e7-be74-f00275ef8476'

data = {
    "email": "a.valeev@litebox.ru",
    "password": "mypos123",
    "syncId": syncId
}

# линкуем
r = requests.post(url + url_link, json=data, verify=False)
print(r.text)
# {"linkedAt":1487167821378,"companyId":344,"token":"_2btzrhizog'km11t1btlzmnnq85lmyfc*n644.ml1hsxs_fa*su9d0t9f.7pxuv"}
