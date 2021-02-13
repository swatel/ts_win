# -*- coding: utf-8 -*-
from orm_v2.SQLModel import *
from orm_v2.utils.json.JsonReader import JsonReader
from orm_v2.utils.json.JsonWriter import JsonWriter


class Object(SQLModel, JsonReader, JsonWriter):
    _table_name = 'OBJECT'

    obj_id = IntColumn('objid', primary_key=True)
    obj_type = CharColumn('objtype')
    code = StringColumn('code')
    name = StringColumn('name', real_column=False)
    kpp = StringColumn('kpp', real_column=False)
    inn = StringColumn('inn', real_column=False)
    phone = StringColumn('phone', real_column=False)
    email = StringColumn('email', real_column=False)
    address = StringColumn('address', real_column=False)
    address_real = StringColumn('addressreal', real_column=False)
    external_id = StringColumn('externalid', real_column=False)
    external_code = StringColumn('externalcode', real_column=False)
    external_type = StringColumn('externaltype', real_column=False)

    converters = {
        'json': {
            'obj_id': 'objid',
            'obj_type': 'objtype',
            'address_real': 'addressreal',
            'external_id': 'externalid',
            'external_code': 'externalcode',
            'external_type': 'externaltype'
        }
    }

    def __str__(self):
        return 'obj_id = ' + (str(self.obj_id) if self.obj_id else 'None') + ', ' +\
               'obj_type = ' + (self.obj_type if self.obj_type else 'None') + ', ' +\
               'code = ' + (self.code if self.code else 'None') + ', ' +\
               'name = ' + (self.name if self.name else 'None') + ', ' +\
               'kpp = ' + (self.kpp if self.kpp else 'None') + ', ' +\
               'inn = ' + (self.inn if self.inn else 'None') + ', ' +\
               'phone = ' + (self.phone if self.phone else 'None') + ', ' +\
               'email = ' + (self.email if self.email else 'None') + ', ' +\
               'address = ' + (self.address if self.address else 'None') + ', ' +\
               'address_real = ' + (self.address_real if self.address_real else 'None') + ', ' +\
               'external_id = ' + (self.external_id if self.external_id else 'None') + ', ' +\
               'external_code = ' + (self.external_code if self.external_code else 'None') + ', ' +\
               'external_type = ' + (self.external_type if self.external_type else 'None')
