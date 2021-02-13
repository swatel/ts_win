# -*- coding: utf-8 -*-




import requests
import json

from rbsqutils import json_encode_1251
from rbsqutils import convToUTF8
from rbsqutils import check_unic_barcode
from rbsqutils import current_date
import krconst as c


class Api(object):

    __url = "https://kabinet.dreamkas.ru/api"
    __supports = {
        'export_store': True,
        'import_store': True,
        'import_cashdesk': True,
        'export_method_gwares': True, # метод для получения товаров для экспорта
        'export_gwares': True,
        'import_docs': True
      # 'import_gwares': False,
      # 'export_docs': False,

      # 'import_store': True
    }

    def __init__(self, parent_obj, params={}):
        self.parent_obj = parent_obj
        self.token = params.get('access_token')
        # self.company_id = None
        # self.syncId = params.get('syncId')
        # self.email = params.get('email')
        # self.password = params.get('password')

        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % self.token
        }

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

    def get_store(self):
        """
        Запрос магазинов
        @return: list
        """

        result = []

        path = '/shops'

        response = self.__get(path)
        if response:     # list
            if response[0]:
                for p, v in enumerate(response[1]):
                    store = json_encode_1251(v)
                    result.append({
                        'externalid': str(store['id']),
                        'name': store['name']
                    })

        return result

    def set_store(self, obj):
        """
        Добавление магазинов
        @return: list
        """

        result = []

        data = {
            'name': (obj.name).decode('cp1251'),
            # u'name': convToUTF8(obj.name),
            'sort': 0
        }

        path = '/shops'

        if obj.external_id:
            response = self.__patch(path, obj.external_id, data)
        else:
            response = self.__post(path, data)

        if response:     # list
            if response[0]:
                if response[1]:
                    store = json_encode_1251(response[1])
                    result.append({
                        'externalid': str(store['id']),
                        'name': store['name']
                    })

        return result

    def get_cashdesk(self):
        """
        Запрос касс
        @return: list
        """

        result = []

        path = '/devices'

        response = self.__get(path)
        if response:     # list
            if response[0]:
                for p, v in enumerate(response[1]):
                    equip = json_encode_1251(v)
                    result.append({
                        'externalid': str(equip['id']),
                        'name': equip['name'],
                        'external_obj_id': str(equip['groupId'])
                    })

        return result

    def export_method_gwares(self, last_date):
        """
        Метод получения справочника товарв
        @return: 
        """
        result = []

        sql_text = "select mp.* from Q_API_DREAMKASS_PRODUCTS(?,?,?,'S') mp"
        sql_params = ['DREAMKASS', None, last_date]
        r = self.parent_obj.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='many')
        if r['status'] == c.kr_sql_error:
            self.parent_obj._exit(message='Не удалось получить товары и группы')
        else:
            result = r['datalist']
        return result

    def set_gwares(self, deviceid, data):

        result = []
        barcode_uses = []
        for itm in data:
            producttype = itm['producttype']
            meta = None
            if producttype == 'ALCOHOL':
                alcoholProductKindCode= None
                if itm['alcoholProductKindCode']:
                    alcoholProductKindCode = str(itm['alcoholProductKindCode'])

                tareVolume = None
                if itm['tareVolume']:
                    tareVolume = int(itm['tareVolume'] * 1000)
                else:
                    tareVolume = 0

                alcoholByVolume = None
                if itm['alcoholByVolume']:
                    alcoholByVolume = int(itm['alcoholByVolume'] * 10)
                else:
                    alcoholByVolume = 0

                alcocodes = ''
                if itm['alcocodes']:
                    alcocodes = itm['alcocodes']
                meta = {
                    'code': alcocodes,
                    'typeCode': alcoholProductKindCode,
                    'volume': tareVolume,
                    'alc': alcoholByVolume
                }

            quantity = None
            if itm['is_weight_wares'] == '0':
                quantity = 1000
            else:
                quantity =  1

            tax = None
            if itm['tax'] in ('VAT_0', 'NO_VAT'):
                tax = 0
            if itm['tax'] == 'VAT_18':
                tax = 18
            if itm['tax'] == 'VAT_10':
                tax = 10
            if itm['tax'] == 'VAT_18_118':
                tax = 118
            if itm['tax'] == 'VAT_10_110':
                tax = 110

            if itm['barcodes']:
                barcodes = itm['barcodes'].split(',')
                i_delta = 0
                i_del = 0
                for itm_b in barcodes:
                    if len(itm_b) > 13:
                        barcodes.pop(i_del - i_delta)
                        i_delta += 1
                    else:
                        flag_use, barcode_uses = check_unic_barcode(barcode_uses, itm_b)
                        if not flag_use:
                            barcodes.pop(i_del - i_delta)
                            i_delta += 1
                    i_del += 1
            else:
                barcodes = []

            prices = None
            if itm['price_json']:
                prices = json.loads(itm['price_json'])

            wares = {
                'name': itm['name'].decode('cp1251'),
                'type': producttype,
                'departmentId': None,
                'tax': tax,
                'quantity': quantity,
                'barcodes': barcodes,
                'price': int(itm['price']*100),
                'prices': prices,
                'meta':meta
            }

            path = '/products'
            if itm['uuid']:
                ret = self.__patch(path, itm['uuid'], wares)
            else:
                ret = self.__post(path, wares)
            if ret:
                if ret[0]:
                    if not itm['uuid']:
                        if ret[1]:
                            tmp = json_encode_1251(ret[1])
                            self.parent_obj.external_inssel(str(tmp['id']), itm['id'], table_name='GOODS')
                            # result.append({
                            #     'waresid':itm['id'],
                            #     'externalid': str(tmp['id'])
                            # })
        return True

        # todo Щеглов: переделать на модели

    def __import_wares(self, externalid, externalcode, codegwares, namegwares, unitid, taxid, waresgrid, flag, deletemarker,
                      awaresid, exchange_task_code):
        """
        Поиск и добавление товара при необходимости
        """
        sql_text = "select waresid from Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?)"
        sql_params = [externalid, externalcode, codegwares, namegwares, unitid, taxid, waresgrid, flag, deletemarker,
                      awaresid, exchange_task_code]
        r = self.parent_obj.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
        if r['status'] == c.kr_sql_error:
            self.parent_obj._exit(message='Ошибка поиска товара!')
            return None
        else:
            return r['datalist']['waresid']

    def get_docs(self, cashdesk, last_date):
        """
        Получаем документы
        @return: 
        """

        str_current_date = current_date(self.parent_obj.db, self.parent_obj.layer_code)

        for itm in  cashdesk:
            deviceid = itm['externalid']
            equipment_hash = itm['equipment_hash']
            path = '/receipts'
            data = {
                'from': str(last_date).replace(' 00:00:00','T00:00:00'),
                'to': str(str_current_date).replace(' 00:00:00','T23:59:59'),
                'limit': 1000,
                'devices': deviceid
            }
            ret = self.__get(path, data)
            if ret:
                if ret[0]:
                    if ret[1]:
                        docs = sorted(ret[1]['data'], key=lambda k: k['localDate'])
                        if len(docs) == 0:
                            return True
                        shiftId = None
                        session_id = None
                        # todo Щеглов: переделать на модели orm
                        session_external_id = None
                        for doc in docs:
                            # новая сессия
                            if shiftId != doc['shiftId']:
                                # Обнуляем сессию
                                if not session_external_id:
                                    session_external_id = deviceid + str(doc['shiftId'])
                                shiftId = doc['shiftId']
                                if session_id:
                                    # закрываем сессию
                                    sql_text = "select sessionid from Q_API_SESSION_CLOSE(?,?,?,?)"
                                    sql_params = [session_id,
                                                  doc['localDate'], # заакрытие
                                                  None, # сумма на конец
                                                  session_external_id
                                                  ]
                                    r = self.parent_obj.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
                                    if r['status'] == c.kr_sql_error:
                                        self.parent_obj._exit(message='Ошибка закрытия сессии')
                                        return False
                                    else:
                                        session_id = r['datalist']['SESSIONID']
                                session_id = None
                                session_external_id = deviceid + str(doc['shiftId'])
                                sql_text = 'select sessionid from Q_API_SESSION_START(?,?,?,?,?,?)'
                                sql_params = [None, # пользователь
                                              equipment_hash,
                                              doc['localDate'],
                                              None,  # Сумма на начало смены - такого касса не умеет
                                              session_external_id,  # external_id
                                              'DK'  # Если будет F - то только поиск сессии
                                             ]
                                r = self.parent_obj.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
                                if r['status'] == c.kr_sql_error:
                                    self.parent_obj._exit(message='Ошибка открытия сессии')
                                    return False
                                else:
                                    session_id = r['datalist']['SESSIONID']

                            # шапка документа
                            if doc['type'].encode('cp1251') == 'SALE':
                                doc_type = 'SALE'
                            else:
                                doc_type = 'RET'

                            sql_text = "select docid from Q_API_CASH_MYDOC(?,?,?,?,?,?,?,?)"
                            sql_params = [equipment_hash,
                                          doc['localDate'],
                                          None, # номер документа
                                          None, # кассир
                                          None,  # тип оплаты чека (null, сейчас не используется)
                                          doc_type,  # тип операции (продажа = SALE, возврат = RET)
                                          doc['id'].encode('cp1251'),  # external_id
                                          None,  # ид. чека продажи (если делается чек возврата с этой продажи)
                                         ]
                            r = self.parent_obj.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
                            if r['status'] == c.kr_sql_error:
                                self.parent_obj._exit(message='Ошибка записи шапки чека')
                                return False
                            else:
                                doc_id = r['datalist']['DOCID']
                                for cargo in doc['positions']:
                                    if cargo['type'].encode('cp1251') in ('ALCOHOL', 'COUNTABLE'):
                                        unit_id = 55
                                    else:
                                        unit_id = 15
                                    wares_id = self.__import_wares(str(cargo['id']), None, None, cargo['name'].encode('cp1251'), unit_id, None, None, 'A', '0', None,
                                                                   'DREAMKASS')
                                    if not wares_id:
                                        return False

                                    quantity = cargo['quantity'] / 1000
                                    price = cargo['price'] / 100
                                    sql_text = "select s.cargosalesid " \
                                               "  from API_CASH_MYDOCGOODS(?,?,?,?,?,?) s"
                                    sql_params = [
                                        doc_id,
                                        wares_id,
                                        quantity,
                                        price,
                                        quantity * price,
                                        None
                                    ]
                                    r = self.parent_obj.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
                                    if r['status'] == c.kr_sql_error:
                                        self.parent_obj._exit(message='Ошибка записи позиции чека')
                                        return False
                                # оплата
                                for payment in doc['payments']:
                                    if payment['type'].encode('cp1251') == 'CASH':
                                        payment_type = 'CASH'
                                    else:
                                        payment_type = 'NONCASH'
                                    payment_type += doc_type
                                    sql_text = "select * from Q_API_CASH(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                                    sql_params = [
                                        doc_id,
                                        session_id,
                                        None,
                                        payment_type,
                                        doc['localDate'],
                                        payment['amount']/100,
                                        None,
                                        None,
                                        None,
                                        None,
                                        None,
                                        'RUB',
                                        str(doc_id),
                                        None,
                                        None
                                    ]
                                    r = self.parent_obj.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
                                    if r['status'] == c.kr_sql_error:
                                        self.parent_obj._exit(message='Ошибка записи оплаты чека')
                                        return False
        return True

    # todo Щеглов: Подумать как унифицировать данные функции для всех интеграций
    def __post(self, path, data):

        r = requests.post(self.__url + path, json=data, headers=self.headers)
        if r.status_code in (200, 201) and r.headers.get('Content-Type', '').startswith('application/json'):
            res = json.loads(r.text)
            return True, res
        elif r.status_code == 400 and ('Invalid barcode checksum' in r.text or 'ШК' in r.text):
            self.parent_obj.log_file('Неверный ШК. Добавляем товар без ШК.', terms=1)
            data['barcodes'] = None
            return self.__post(path, data)
        else:
            self.parent_obj._exit(message=data)
            self.parent_obj._exit(message=json.dumps(data))
            self.parent_obj._exit(message=(r.text).encode('cp1251'))
            return False, None

    def __get(self, path, params=None):

        r = requests.get(self.__url + path, params=params,  headers=self.headers)
        if r.status_code == 200 and r.headers.get('Content-Type', '').startswith('application/json'):
            res = json.loads(r.text)
            return True, res
        else:

            self.parent_obj._exit(message=(r.text).encode('cp1251'))
            return False, None

    def __patch(self, path, data_id, data):
        """
        Обновление данных
        @param path: путь
        @param data_id: ид
        @param data: данный
        @return: результат
        """

        r = requests.patch(self.__url + path + '/' + data_id, json=data, headers=self.headers)
        if r.status_code in (200, 204) and r.headers.get('Content-Type', '').startswith('application/json'):
            res = json.loads(r.text)
            return True, res
        elif r.status_code == 204:
            return True, None
        elif r.status_code == 404:
            self.parent_obj.log_file('Данные для обновления не найдены, возможно были удалены. Добавляем.', terms=1)
            return self.__post(path, data)
        elif r.status_code == 400 and ('Invalid barcode checksum' in r.text or 'ШК' in r.text):
            self.parent_obj.log_file('Неверный ШК. Обновляем товар без ШК.', terms=1)
            data['barcodes'] = None
            return self.__patch(path, data_id, data)
        else:
            self.parent_obj._exit(message=data)
            self.parent_obj._exit(message=json.dumps(data))
            self.parent_obj._exit(message=(r.text).encode('cp1251'))
            return False, None
