# -*- coding: utf-8 -*-
from orm_v2.models.Object import Object

if __name__ == "__main__":
    json_obj = {
        'obj_id': 1,
        'name': 'ООО ТК «ВИНСЕНТ»',
        'obj_type': 'C',
        'code': '1',
        'kpp': '732501001',
        'inn': '7325116226',
        'phone': '8(343)295-18-95',
        'email': 'info@r66.center-inform.ru',
        'address': 'Байконур',
        'address_real': 'Байконур',
        'external_id': None,
        'external_code': '666',
    }

    obj1 = Object.json_reads(json_obj=json_obj)
    print(obj1)

    json_text = '{"code": "1", "phone": "8(343)295-18-95", "inn": "7325116226",' \
                '"address": "Байконур", "address_real": "Байконур", "kpp": "732501001", "obj_type": "C",' \
                '"name": "ООО ТК «ВИНСЕНТ»", "obj_id": 1, "external_code": "666", "external_id": null,' \
                '"email": "info@r66.center-inform.ru"}'

    obj2 = Object.json_reads(text=json_text)
    print(obj2)

    print(obj1.json_write(encoding='utf-8'))

    json_text_u = '{"code": "1", "phone": "8(343)295-18-95", "inn": "7325116226",' \
                '"address": "Байконур", "address_real": "Байконур", "kpp": "732501001", "obj_type": "C",' \
                '"name": "ООО ТК «ВИНСЕНТ»", "obj_id": 1, "external_code": "666", "external_id": null,' \
                '"email": "info@r66.center-inform.ru"}'.decode('cp1251').encode('utf8')

    obj3 = Object.json_reads(text=json_text_u, encoding='utf8')
    print(obj3)

    print(obj3.json_write())
