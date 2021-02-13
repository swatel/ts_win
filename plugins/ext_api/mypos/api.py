# -*- coding: utf-8 -*-


import requests
import json
import string

from rbsqutils import json_encode_1251
from rbsqutils import barcode_int
from orm.utils import myposreader

from orm.models.mypos.SessionOpen import SessionOpen
from orm.models.mypos.SessionClose import SessionClose
from orm.models.mypos.SaleOpen import Sale
from orm.models.mypos import Cargo as Cg
from orm.models.mypos import Payment as Pa


VERSION = '0.0.2.0'

headers = {
    'Host': 'cloud.mypos.ru',
    'Content-Type': 'application/json',
    'Cache-Control': 'no-cache'
}


class Api(object):

    __url = "https://cloud.mypos.ru/api"
    __supports = {'export_shop': False,
                  'import_shop': False,
                  'export_cashdesk': False,
                  'import_cashdesk': True,
                  'export_gwares': True,
                  'import_gwares': False,
                  'export_docs': False,
                  'import_docs': True
                  }

    def __init__(self, parent_obj, params={}):
        self.parent_obj = parent_obj
        self.token = None
        self.company_id = None
        self.syncId = params.get('syncId')
        self.email = params.get('email')
        self.password = params.get('password')

    def supports(cls, method, mode=None):
        """
        Поддерживает ли плагин метод method
        @param method: Имя метода
        @param mode: Режим работы
        @return:
        """
        if mode is not None:
            return False
        else:
            return method in cls.__supports

    def auth(self):

        if self.syncId and self.token:
            return True

        path = '/sync/link'
        data = {
            "email": self.email,
            "password": self.password,
            "syncId": self.syncId
        }

        response = self.__post(path, data)
        if response:
            if response[0]:
                self.token = response[1].get('token')
                linkedat = response[1].get('linkedAt')
                self.company_id = response[1].get('companyId')
                return self.syncId and self.token
            else:
                return False
        else:
            return False

    def get_cashdesk(self):
        """
        Запрос касс
        @return: list
        """

        result = []
        if not self.auth():
            return

        path = '/company/%d/sync/shifts' % self.company_id
        data = {
            "syncId": self.syncId,
            "token": self.token,
            "empty": True
        }

        response = self.__post(path, data)
        if response:     # list
            if response[0]:
                for p, v in enumerate(response[1]):
                    equip = json_encode_1251(v)
                    result.append({
                        'externalid': equip['deviceId'],
                        'name': equip['name']
                    })

        return result

    def put_cashdesk(self, data):
        pass

    def get_gwares(self):
        pass

    def put_gwares(self, deviceid, data):

        if not self.auth():
            return

        snull = lambda x: '' if not x else x

        wares = []
        groups = {}

        for i in data:
            if i['isgroup']:
                groups[i['id']] = i
            elif i['allowtosell']:
                wares.append(i)

        gwares = ['##@@&&$$$CLR {COM_GR_TOV} $$$ADD', '#']
        attrib = []

        for i in wares:
            discount = i['minprice']/i['price']
            for barcode in snull(i['barcodes']).split(','):
                gwares.append('%s;%s;%s;;%.2f;;;%s;;%.1f;;;;;;%s;1;' % (i['code'], barcode_int(barcode), i['name'], i['price'], i['is_weight_wares'], discount, i['parentid']))
                if i['alcoholproductkindcode']:
                    gwares.append('<goods_attr id="%s" attr_id="22">%s</goods_attr>' % (i['code'], i['alcoholproductkindcode']))
                if i['tarevolume']:
                    gwares.append('<goods_attr id="%s" attr_id="23">%s</goods_attr>' % (i['code'], i['tarevolume']))
                if i['PROP_PDFSTAMP']:
                    gwares.append('<goods_attr id="%s" attr_id="24">%s</goods_attr>' % (i['code'], str(i['PROP_PDFSTAMP'])))
                if i['PRODUCERINN']:
                    gwares.append('<goods_attr id="%s" attr_id="25">%s</goods_attr>' % (i['code'], i['PRODUCERINN']))
                if i['PRODUCERKPP']:
                    gwares.append('<goods_attr id="%s" attr_id="26">%s</goods_attr>' % (i['code'], i['PRODUCERKPP']))
                if i['prop_alcohol']:
                    gwares.append('<goods_attr id="%s" attr_id="27">%s</goods_attr>' % (i['code'], i['prop_alcohol']))
            pid = i['parentid']
            while pid:
                g = groups[pid]
                gs = '%s;;%s;;;;;;;;;;;;;%s;0;' % (snull(g['id']), g['name'], snull(g['parentid']))
                if gs not in gwares:
                    gwares.append(gs)
                pid = g['parentid']
            # attributes

        path = '/company/%s/sync/products' % self.company_id
        data = {
            "syncId": self.syncId,
            "token": self.token,
            "deviceId": str(deviceid)
        }

        data['products'] = (string.join(gwares, '\r\n')).decode('cp1251')
        ret = self.__post(path, data)
        result = None
        if ret:
            if ret[0]:
                result = json_encode_1251(ret[1])
        return result

    def get_docs(self, cashdesk):
        """
        Получаем документы
        @return: 
        """

        folder = []
        for itm in  cashdesk:
            folder.append({
                "deviceId": itm['externalid']
            })

            path = '/company/%s/sync/shifts' % self.company_id
            data = {
                "syncId": self.syncId,
                "token": self.token,
                "empty": False,
                "folder": folder
            }
            equipment_hash = itm['equipment_hash']
            ret = self.__post(path, data)
            result = None
            if ret:
                if ret[0]:
                    for data in ret[1]:
                        if len(data['content']) > 0:
                            with myposreader(data['content'].encode('cp1251')) as reader:
                                models = reader.read()
                            has_doc_id = (Cg.Cargo, Pa.Payment)
                            # Список моделей, для которых нужена сессия
                            # пока нет
                            has_session_id = ()
                            if models is not None:
                                session_id = None
                                doc_id = None
                                for model in models:
                                    # Разборки с сессией
                                    if session_id is None:
                                        # Выясняем сессию кассы
                                        params = {'equipment_hash': equipment_hash}
                                        if isinstance(model, SessionOpen):
                                            # проверяем, может сессия уже была создана ранее
                                            params['flag'] = 'F'
                                            if model.save(self.parent_obj.execute_sql, params=params):
                                                session_id = model.session_id
                                            else:
                                                return self.parent_obj._exit(message='Ошибка при открытии смены.' + "\n" +
                                                                          model.last_db_execute_error)
                                            # если нет, создаем
                                            if session_id is None:
                                                params['flag'] = ''
                                                if model.save(self.parent_obj.execute_sql, params=params):
                                                    session_id = model.session_id
                                                else:
                                                    return self.parent_obj._exit(message='Ошибка при открытии смены.' + "\n" +
                                                                                         model.last_db_execute_error)
                                        if session_id is None:
                                            return self.parent_obj._exit(message='Не удалось открыть сессию для кассы')
                                    else:
                                        if isinstance(model, SessionClose):
                                            model.session_id = session_id
                                            if model.save(self.parent_obj.execute_sql):
                                                session_id = None
                                            else:
                                                return self.parent_obj._exit(message='Ошибка при закрытии смены.' + "\n" +
                                                                          model.last_db_execute_error)
                                    if isinstance(model, has_session_id):
                                        model.session_id = session_id
                                        if not model.save(self.parent_obj.execute_sql):
                                            return self.parent_obj._exit(message='Ошибка при проведении операции.' + "\n" +
                                                                      model.last_db_execute_error)
                                    # Встретили документ
                                    if isinstance(model, Sale):
                                        # Продажа
                                        params = {'equipment_hash': equipment_hash}
                                        if model.save(self.parent_obj.execute_sql, params=params, auto_commit=False):
                                            doc_id = model.doc_id
                                        else:
                                            self.parent_obj.db.rollback()
                                            return self.parent_obj._exit(message='Ошибка при создании чека.' + "\n" +
                                                                      model.last_db_execute_error)
                                    else:
                                        if isinstance(model, has_doc_id):
                                            model.doc_id = doc_id
                                            model.session_id = session_id
                                            if not model.save(self.parent_obj.execute_sql):
                                                self.parent_obj.db.rollback()
                                                return self.parent_obj._exit(message='Ошибка при создании продажи/оплаты.' + "\n" +
                                                                          model.last_db_execute_error)
                                        else:
                                            # Встретили операцию, которая не входит в документ
                                            # Закроем транзацию
                                            self.parent_obj.db.commit()
                                            #  - сбросим doc_id
                                            doc_id = None
        return result

    def put_docs(self, data):
        pass

    def __post(self, path, data):

        r = requests.post(self.__url+path, json=data, headers=headers)
        if r.status_code == 200 and r.headers.get('Content-Type', '').startswith('application/json'):
            res = json.loads(r.text)
            return True, res
        else:
            self.parent_obj.log_file((r.text).encode('cp1251'))
            res = None
            return False, res
