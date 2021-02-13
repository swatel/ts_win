# -*- coding: utf-8 -*-

import os
import json
import requests
import plugins.model.waresgroup as wg
import plugins.model.gwares as g
import plugins.model.document as d
import plugins.model.documentgood as dg
import plugins.model.company as c
import plugins.model.tax as tx
from datetime import datetime
from utils.decorator import timer


VERSION = '0.0.1.1'

json_headers = {'Host': 'app.ecwid.com',
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache'}
jpg_headers = {'Host': 'app.ecwid.com',
               'Content-Type': 'image/jpg',
               'Cache-Control': 'no-cache'}


class Api(object):
    """
        Реализация методов API Ecwid
        http://api.ecwid.com/#using-ecwid-api

        API VERSION
        This document describes Ecwid REST API v.3

        HTTPS
        All requests are done via HTTPs. Requests via insecure HTTP are not supported.

        UTF-8
        Ecwid API works with UTF-8 encoded data. Please make sure everything you send over in API calls also uses UTF-8.

        JSON
        All data received from API and submitted to API is JSON.
    """

    __url = 'https://app.ecwid.com/api/v3/'
    __step = 100
    __access_token = None;
    __store_id = None
    __supports = {'export_profile': True,
                  'export_taxes': True,
                  'export_wares_groups': True,
                  'export_gwares': True,
                  'export_wares_rests': True,
                  'orders': True,
                  }

    def __init__(self, parent_obj, params={}):
        self.parent_obj = parent_obj
        # Токен для доступа
        if 'access_token' in params:
            self.__access_token = params['access_token']
        else:
            raise Exception('Param "access_token" is empty.')
        if 'store_id' in params:
            self.__store_id = str(params['store_id'])
        else:
            raise Exception('Param "store_id" is empty.')

    @classmethod
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

    def get_wares_groups(self, last_date, external_id=None):
        """
        Получение товарных групп из Ecwid
        :param last_date: Дата/время последнего обмена в UTC
        :param external_id: ид
        :return: массив plugins/model/waresgroup
        """
        # Ниже вариант с обходом с корня, получаем отсортированную таблицу (как дерево)
        # return self.__get_wares_groups(parent_id=0, hidden=True)
        # Ниже вариант с обходом сразу всех категорий, без гарантированных сортировок
        return self.__get_wares_groups(hidden=True, last_date=last_date, external_id=external_id)

    def get_gwares(self, utc_last_date=None, external_id=None):
        """
        Получение товарных групп из Ecwid
        :param utc_last_date:  Дата/время последнего обмена в UTC
        :param external_id:  ид товара, для точечного импорта
        :return: plugins/model/gwares
        """
        gwares = []
        loop = True
        offset = 0
        params = {}
        if external_id:
            url_name = 'products/' + str(external_id)
        else:
            url_name = 'products'
            if utc_last_date is not None:
                utc_date = utc_last_date.strftime("%Y-%m-%d %H:%M:%S UTC")
                #params['createdFrom'] = utc_date
                params['updatedFrom'] = utc_date

        while loop:
            params['offset'] = offset
            response = self.__get(url_name, params)
            # response = self.__get('products', params)
            if self.check_response_ok(response):
                data = json.loads(response.text)
                if external_id:
                    if len(data) > 0:
                        wares = self.__json_2_wares(self.parent_obj, data)
                        gwares.append(wares)
                    else:
                        break
                    loop = False
                else:
                    count = data['count']
                    data_items = data['items']

                    if int(count) == 0:
                        # Товаров нет - выход
                        break
                    else:
                        for item in data_items:
                            # Добавить группу и сразу же пойти по ее потомкам, для построения дерева
                            wares = self.__json_2_wares(self.parent_obj, item)
                            gwares.append(wares)
                            # if parent_id is not None:
                            #     # Если обходим один узел, либо с корня - тогда рекурсия
                            #     wares_groups += self.__get_wares_groups(hidden, group.external_id)
                    # Если за итерацию раз не получили необходимые данные
                    offset += int(count)
                    # То будет еще одна
                    loop = int(data['total']) > offset
            else:
                return False
        return gwares

    def update_profile(self, data):
        """
        Обновление профиля пользователя
        :param data: Данные профиля
        :return:
        """
        profile = {'company': {'companyName': data['company_name']}}
        # Информация о магазине
        response = self.__put('profile', {}, json.dumps(profile, encoding='cp1251'))
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при обновлении профиля ' + str(response.status_code) + ' ' + response.reason)
        return response.status_code == 200

    def update_taxes(self, data):
        """
        Обновление налоговых ставок и стран
        :param data: Данные
        :return:
        """
        profile = {'zones': [], 'taxes': []}
        zones = []
        for tax in data:
            """tax = taxid, name, rate, countryid, countrycode, countryname """
            if tax.countryid not in zones:
                profile['zones'].append({'id': str(tax.countryid),
                                         'name': tax.countryname,
                                         'countryCodes': [tax.countrycode]})
                zones.append(tax.countryid)
            profile['taxes'].append({'id': tax.taxid,
                                     'name': tax.name,
                                     'enabled': True,
                                     'includeInPrice': True,
                                     'appliedByDefault': True,
                                     'rules': [{'zoneId': str(tax.countryid), 'tax': tax.rate}]
                                     })
        response = self.__put('profile', {}, json.dumps(profile, encoding='cp1251'))
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при обновлении налоговых ставок ' + str(response.status_code) + ' ' + response.reason)
        return response.status_code == 200

    def add_wares_group(self, wares_group):
        """
        Добавление товарной группы
        :param wares_group: plugins/model/waresgroup
        :return: boolean
        """
        group = self.__wares_group_2_dict(wares_group)
        response = self.__post('categories', {}, json.dumps(group, encoding='cp1251'))
        data = json.loads(response.text)
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при добавлении товарной группы: ' + str(response.status_code) + ' ' + response.reason)
        else:
            wares_group.external_id = data['id']
        return response.status_code == 200

    def update_wares_group(self, wares_group):
        """
        Обновление товарной группы
        :param wares_group: plugins/model/waresgroup
        :return: boolean
        """
        group = self.__wares_group_2_dict(wares_group)
        del group['id']  # id не передаем
        response = self.__put('categories/'+str(wares_group.external_id), {}, json.dumps(group, encoding='cp1251'))
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при изменении товарной группы: ' + str(response.status_code) + ' ' + response.reason)
        # data = json.loads(response.text)
        # data['updateCount']
        return response.status_code == 200

    def delete_wares_group(self, wares_group):
        """
        Удаление товарной группы
        :param wares_group: plugins/model/waresgroup
        :return: boolean
        """
        response = self.__delete('categories/'+str(wares_group.external_id), {})
        data = json.loads(response.text)
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при удалении товарной группы: ' + str(response.status_code) + ' ' + response.reason)
        return response.status_code == 200

    def add_goods(self, goods):
        """
        Добавление товара
        :param goods: plugins/model/goods
        :return: boolean
        """
        goods_dict = self.__goods_2_dict(goods)
        response = self.__post('products', {}, json.dumps(goods_dict, encoding='cp1251'))
        data = json.loads(response.text)
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при добавлении товара: ' + str(response.status_code) + ' ' + response.reason)
        else:
            goods.external_id = data['id']
            if goods.picture is not None:
                self.upload_image(goods.external_id, goods.picture)
        return response.status_code == 200

    def update_goods(self, goods):
        """
        Обновление товара
        :param goods: plugins/model/goods
        :return: boolean
        """
        goods_dict = self.__goods_2_dict(goods)
        # del goods['id']  # id не передаем
        response = self.__put('products/'+str(goods.external_id), {}, json.dumps(goods_dict, encoding='cp1251'))
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при изменении товара: ' + str(response.status_code) + ' ' + response.reason)
        else:
            if goods.picture is not None:
                self.upload_image(goods.external_id, goods.picture)
        return response.status_code == 200

    def upload_image(self, product_id, filename):
        k_conf = self.parent_obj.parent.k_conf
        if k_conf.engine_dir_files is None:
            self.parent_obj.log_file('Ошибка при изменении изображения товара: не указан путь к папке изображений')
            return None
        filename_full = os.path.normpath(os.path.join(k_conf.engine_dir_files,
                                                      self.parent_obj.layer_code,
                                                      'image',
                                                      'small',
                                                      filename))
        # todo Щеглов: Добавить проверку на существование файла
        with open(filename_full, mode='rb') as picture_file:  # b is important -> binary
            file_content = picture_file.read()
        response = self.__post('products/' + str(product_id) + '/image', {}, file_content, headers=jpg_headers)
        if response.status_code != 200:
            self.parent_obj.log_file(
                'Ошибка при изменении изображения товара: ' + str(response.status_code) + ' ' + response.reason)
        return response

    def update_wares_rest(self, rest):
        """
        Обновление остатков товара
        :param goods: plugins/model/goods
        :return: boolean
        """
        rest_dict = self.__rest_2_dict(rest)
        # del goods['id']  # id не передаем
        response = self.__put('products/'+str(rest.external_id), {}, json.dumps(rest_dict, encoding='cp1251'))
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при изменении товарного остатка: ' + str(response.status_code) + ' ' + response.reason)
        # data = json.loads(response.text)
        # data['updateCount']
        return response.status_code == 200

    def delete_goods(self, goods):
        """
        Удаление товара
        :param goods: plugins/model/goods
        :return: boolean
        """
        response = self.__delete('products/'+str(goods.external_id), {})
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка при удалении товара: ' + str(response.status_code) + ' ' + response.reason)
        return response.status_code == 200

    def get_orders(self, timestamp):
        """
        Получение документов
        @param timestamp: Дата последнего обмена (UNIX TIMESTAMP)
        @return: list of plugins/model/document
        """
        params = {}
        if timestamp is None:
            # Забираем все
            return self.get_orders_by_params(params)
        else:
            documents = []
            # Сперва забираем измененные
            params['updatedFrom'] = timestamp
            res, docs = self.get_orders_by_params(params)
            if res:
                documents.extend(docs)
                # Потом забираем созданные ??
                # params.pop('updatedFrom', None)
                # params['createdFrom'] = timestamp
                # documents.extend(self.get_orders_by_params(params))
                # res, docs = self.get_orders_by_params(params)
                # if res:
                #     documents.extend(docs)
            return res, documents

    def get_orders_by_params(self, params):
        documents = []
        loop = True
        offset = 0
        while loop:
            params['offset'] = offset
            response = self.__get('orders', params)
            if self.check_response_ok(response):
                data = json.loads(response.text)
                if int(data['count']) == 0:
                    # Документов нет - выход
                    break
                else:
                    for item in data['items']:
                        document = self.__json_2_document(self.parent_obj, item)
                        documents.append(document)
                # Если за итерацию раз не получили необходимые данные
                offset += int(data['count'])
                # То будет еще одна
                loop = int(data['total']) > offset
            else:
                return False, []
        return True, documents

    def get_taxes(self):
        """
        Получение налоговых ставок
        :return: plugins/model/tax
        """
        response = self.__get('profile', {})
        if self.check_response_ok(response):
            taxes = []
            profile = json.loads(response.text)
            for profile_tax in profile['taxes']:
                if 'rules' not in profile_tax or len(profile_tax['rules']) == 0:
                    profile_tax['tax'] = profile['defaultTax']
                    profile_tax['zoneId'] = 0
                    taxes.append(self.__json_2_tax(profile_tax))
                else:
                    for zone_tax in profile_tax['rules']:
                        profile_tax['tax'] = zone_tax['tax']
                        # TODO Хмелевский По zoneId пытаться выяснить страну
                        profile_tax['zoneId'] = zone_tax['zoneId']
                        taxes.append(self.__json_2_tax(self.parent_obj, profile_tax))
            return taxes
        else:
            return False

    def __get_wares_groups(self, parent_id=None, hidden=False, last_date=None, external_id=None):
        """
        Получение товарных групп из Ecwid
        :param parent_id: ID родительской группы
        :param hidden: Учитывать скрытые
        :param external_id: ID группы
        :return: plugins/model/waresgroup
        """
        wares_groups = []
        params = {}
        loop = True
        offset = 0

        params['hidden_categories'] = 'true' if hidden else 'false'
        params['offset'] = offset

        if external_id:
            url_name = 'categories/' + str(external_id)
        else:
            url_name = 'categories'
            params['parent'] = parent_id

        while loop:
            response = self.__get(url_name, params)
            if self.check_response_ok(response):
                data = json.loads(response.text)
                if external_id:
                    if len(data) > 0:
                        # Добавить группу и сразу же пойти по ее потомкам, для построения дерева
                        group = self.__json_2_wares_group(self.parent_obj, data)
                        wares_groups.append(group)
                        if group.h_external_id is not None:
                            # Если обходим один узел, либо с корня - тогда рекурсия
                            wares_groups += self.__get_wares_groups(hidden=hidden, parent_id=group.h_external_id)
                    loop = False
                else:
                    if int(data['count']) == 0:
                        # Категория пустая - выход
                        break
                    else:
                        for item in data['items']:
                            # Добавить группу и сразу же пойти по ее потомкам, для построения дерева
                            group = self.__json_2_wares_group(self.parent_obj, item)
                            wares_groups.append(group)
                            if parent_id is not None:
                                # Если обходим один узел, либо с корня - тогда рекурсия
                                wares_groups += self.__get_wares_groups(hidden=hidden, parent_id=group.external_id)
                    # Если за итерацию раз не получили необходимые данные
                    offset += int(data['count'])
                    # То будет еще одна
                    loop = int(data['total']) > offset
            else:
                return False
        return wares_groups

    def check_response_ok(self, response):
        """
        Проверка результата обращения к серверу
        @param response: :class:`Response <Response>` object
        @return: Boolean
        """
        if response.status_code != 200:
            self.parent_obj.log_file('Ошибка получения данных из Ecwid: ' + str(response.status_code) + ' ' \
                                     + response.reason)
        return response.status_code == 200

    @staticmethod
    def order_to_dict(order):
        """
        Заказ в json
        @param order: Заказ
        @return: dict
        """
        order_data = dict()
        order_data['subtotal'] = order.amount
        order_data['total'] = order.amount
        order_data['email'] = order.client.email
        if order.client.external_id is not None:
            order_data['customerId'] = order.client.external_id
        if order.external_paid == 1 and order.external_refunded != 1:
            status1 = 'PAID'
        elif order.external_paid == 1 and order.external_refunded == 1:
            status1 = 'REFUNDED'
        else:
            status1 = 'AWAITING_PAYMENT'
        order_data['paymentStatus'] = status1
        if order.external_paid == 1 and order.external_refunded != 1:
            status2 = 'DELIVERED'
        elif order.external_paid == 1 and order.external_refunded == 1:
            status2 = 'RETURNED'
        else:
            status2 = 'PROCESSING'
        order_data['fulfillmentStatus'] = status2
        order_data['items'] = list()
        for good in order.goods:
            order_good = dict()
            order_good['name'] = good.name
            order_good['productId'] = int(good.w_external_id)
            order_good['price'] = good.price
            order_good['quantity'] = good.amount
            order_data['items'].append(order_good)
        return order_data

    def update_order(self, order):
        """
        Создание заказа
        @param order: Заказ из БД
        @return:
        """
        order_data = self.order_to_dict(order)
        response = self.__post('orders/' + str(order.external_id), {}, json.dumps(order_data, encoding='cp1251'))
        if self.check_response_ok(response):
            return True
        return False

    def create_order(self, order):
        """
        Создание заказа
        @param order: Заказ из БД
        @return:
        """
        order_data = self.order_to_dict(order)
        response = self.__post('orders', {}, json.dumps(order_data, encoding='cp1251'))
        if self.check_response_ok(response):
            doc = json.loads(response.text)
            order.external_id = doc['id']
            return True
        return False

    def get_doc(self, order_id):
        """
        Поиск документа
        @param order_id: Id документа
        @return:
        """
        response = self.__get('orders/' + str(order_id), {})
        if self.check_response_ok(response):
            data = json.loads(response.text)
            return True, self.__json_2_document(self.parent_obj, data)
        elif response.status_code == 404:
            return True, None
        else:
            return False, None

    @staticmethod
    def __wares_group_2_dict(wares_group):
        """
        Конвертирование из модели Товарной группы в dict
        :param wares_group: plugins/model/waresgroup
        :return: dict
        """
        group = {
            'name': wares_group.name
        }
        if wares_group.external_id is not None:
            group['id'] = int(wares_group.external_id)
        if wares_group.parent is not None:
            group['parentId'] = int(wares_group.parent)
        return group

    @staticmethod
    def __json_2_wares_group(parent_obj, item):
        """
        Конвертирование из dict в модель Товарной группы
        :param parent_obj: BasePlugin
        :param item: dict
        :return: plugins/model/waresgroup
        """
        group = wg.WaresGroup(parent_obj)
        group.name = (item['name']).encode('cp1251')
        group.code = str(item['id'])
        group.external_id = item['id']
        group.external_code = str(item['id'])
        if 'parentId' in item:
            group.h_external_id = item['parentId']
            group.h_external_code = item['parentId']
        else:
            group.h_external_id = None
            group.h_external_code = None
        group.status = '1' if item['enabled'] == True else '0'
        return group

    @staticmethod
    def __json_2_document(parent_obj, item):
        """
            Конвертирование из dict в модель Документа
            :param parent_obj: BasePlugin
            :param item: dict
            :return: plugins/model/document
            """
        document = d.Document(parent_obj)
        document.external_id = item['orderNumber']
        document.docdate = datetime.fromtimestamp(item['createTimestamp'])
        document.doctype = 'ORDER'
        document.external_status = item['paymentStatus']
        company = c.Company(parent_obj)
        if 'customerId' in item:
            # Зарегистрированный пользователь
            company.external_id = item['customerId']
        company.email = item['email'].encode('cp1251')
        company.name = item['email'].encode('cp1251')
        document.client = company
        if 'items' in item:
            document.goods = []
            for order_item in item['items']:
                cargo = dg.Docgood(parent_obj)
                cargo.w_external_id = order_item['productId']
                cargo.price = order_item['price']
                cargo.amount = order_item['quantity']
                document.goods.append(cargo)
        return document

    @staticmethod
    def __json_2_wares(parent_obj, item):
        """
        Конвертирование из dict в модель Товара
        :param parent_obj: BasePlugin
        :param item: dict
        :return: plugins/model/gwares
        """
        wares = g.Gwares(parent_obj)
        wares.name = (item['name']).encode('cp1251')
        wares.code = (item['sku']).encode('cp1251')
        wares.external_id = item['id']
        wares.external_code = (item['sku']).encode('cp1251')
        wares.pricesale = item['price']
        # В Эквиде используется только одна единица измерения и учета товаров - это количество в штуках
        # https://www.ecwid.com/forums/showthread.php?t=27660
        wares.unit = 'шт'
        if 'defaultCategoryId' in item:
            wares.wg_external_id = item['defaultCategoryId']
            wares.wg_external_code = item['defaultCategoryId']
        else:
            wares.wg_external_id = None
            wares.wg_external_code = None
        wares.status = '1' if item['enabled'] == True else '0'
        return wares

    @staticmethod
    def __json_2_tax(parent_obj, item):
        """
        Конвертирование из dict в модель Налог
        :param parent_obj: BasePlugin
        :param item: dict
        :return: plugins/model/tax
        """
        tax = tx.Tax(parent_obj)
        tax.name = (item['name']).encode('cp1251')
        tax.rate = item['tax']
        tax.external_id = item['id']
        tax.external_code = item['id']
        # tax.countryid = item['zoneId']
        return tax

    @staticmethod
    def __goods_2_dict(goods):
        """
        Конвертирование из модели Товара в dict
        :param goods: plugins/model/goods
        :return: dict
        """
        obj = {
            'name': goods.name
        }
        if goods.external_id is not None:
            obj['id'] = int(goods.external_id)
        if goods.wexternal_id is not None:
            obj['categoryIds'] = [int(goods.wexternal_id)]
        if goods.code is not None:
            obj['sku'] = goods.code
        if goods.pricesale is not None:
            obj['price'] = goods.pricesale
        obj['enabled'] = goods.status is not None and goods.status == '1'
        obj['unlimited'] = False
        return obj

    @staticmethod
    def __rest_2_dict(rest):
        """
        Конвертирование из модели Товара в dict
        :param goods: plugins/model/goods
        :return: dict
        """
        obj = {}
        if rest.external_id is not None:
            obj['id'] = int(rest.external_id)
        if rest.famount is None:
            obj['quantity'] = 0
        else:
            obj['quantity'] = rest.famount
        return obj

    @staticmethod
    def __json_2_goods(parent_obj, item):
        """
        Конвертирование из dict в модель Товара
        :param parent_obj: BasePlugin
        :param item: dict
        :return: plugins/model/goods
        """
        group = g.Gwares(parent_obj)
        group.name = (item['name']).encode('cp1251')
        group.external_id = item['id']
        return group

    @timer
    def __get(self, path, params):
        """
        Реализация метода GET
        :param path: метод (путь), добавляемый к url API
        :param params: dict параметров запроса (для использования в url)
        :return: requests.Response
        """
        default = {'token': self.__access_token}
        params.update(default)
        return requests.get(self.__url + self.__store_id + '/' + path, params=params)

    @timer
    def __post(self, path, params, data, headers=json_headers):
        """
        Реализация метода POST
        :param path: метод (путь), добавляемый к url API
        :param params: dict параметров запроса (для использования в url)
        :param data: тело запроса
        :return: requests.Response
        """
        default = {'token': self.__access_token}
        params.update(default)
        return requests.post(self.__url + self.__store_id + '/' + path,
                             params=params,
                             data=data,
                             headers=headers)

    @timer
    def __delete(self, path, params):
        """
        Реализация метода DELETE
        :param path: метод (путь), добавляемый к url API
        :param params: dict параметров запроса (для использования в url)
        :return: requests.Response
        """
        default = {'token': self.__access_token}
        params.update(default)
        return requests.delete(self.__url + self.__store_id + '/' + path, params=params)

    @timer
    def __put(self, path, params, json_data, headers=json_headers):
        """
        Реализация метода PUT
        :param path: метод (путь), добавляемый к url API
        :param params: dict параметров запроса (для использования в url)
        :param json_data: тело запроса
        :return: requests.Response
        """
        default = {'token': self.__access_token}
        params.update(default)
        return requests.put(self.__url + self.__store_id + '/' + path,
                            params=params,
                            data=json_data,
                            headers=headers)
