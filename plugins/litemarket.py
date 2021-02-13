# -*- coding: utf-8 -*-
import json
import krconst as c
import six
import requests
import re
import BasePlugin as Bp
from utils.decorator import timer
# Модели
from orm.models.yml.YmlModel import YmlModel
from orm.models.yml.Rest import Rest
from orm.models.yml.Price import Price
from orm.models.yml.Offer import Offer
from orm.models.yml.Currency import Currency
from orm.models.yml.Category import Category


class Plugin(Bp.BasePlugin):
    """

    """
    exchange_task_code = None
    exchange_task_id = None
    obj_id = None
    store_name = None
    url = 'http://cloudretail.ru/Api/'
    login = None
    password = None
    regex_errors = r"<Error>(.*)<\/Error>"
    files_dir = 'files'  # TODO Как получать???

    def run(self):
        # Шаг 1: получить задачу обмена из параметров задания
        self.exchange_task_id = None
        if self.queueparamsxml:
            try:
                self.exchange_task_id = int(self.ParserXML(self.queueparamsxml, 'exchange_task_id'))
            except ValueError:
                self.exchange_task_id = None
        if self.exchange_task_id is None:
            self.log_file('Не указана задача обмена в параметрах очереди заданий', save_log_db=True)
            return False
        # Шаг 2: получить параметрамы обмена
        sql_text = 'select * from Q_API_GETEXCHANGETASK(?,?,?)'
        sql_params = [self.exchange_task_id, None, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения параметров задачи обмена', save_log_db=True)
            return False
        else:
            row = res['datalist']
            if row['status'] != '1':
                self.log_file('Задача обмена заблокирована', save_log_db=True)
                return False
            # Владелец магазина
            self.obj_id = row['objid']
            self.exchange_task_code = row['code']
            # Шаг 3: склеить параметры
            params = {}
            if row['exchangeparams'] is not None:
                try:
                    params = json.loads(str(row['exchangeparams']), encoding='cp1251')
                except:
                    pass
            if row['params'] is not None:
                task_params = json.loads(str(row['params']), encoding='cp1251')
                for key, value in task_params.items():
                    params[key] = value
            params['exchangecode'] = row['exchangecode']
            # Подготовим параметры для передачи дальше
            params = self.json_encode_1251(params)
            self.__process(params)
            # if not self.__process(params):
            #     self['result'] = c.plugin_error

    def __process(self, params):
        """
        Процесс обмена
        :param params: параметры обмена
        :return: истина, если успешно
        """
        if 'store_name' in params:
            self.store_name = params['store_name']
        # Данные для доступа
        if 'login' in params and params['login'] is not None:
            self.login = params['login']
        else:
            self.log_file('Не указан логин для подключения', save_log_db=True)
            return False
        if 'password' in params and params['password'] is not None:
            self.password = params['password']
        else:
            self.log_file('Не указан пароль для подключения', save_log_db=True)
            return False
        return self.__export()

    def __export(self):
        """
        Экспорт в дочернюю систему
        :return: True в случае успеха
        """
        table_id = Offer.fetch_table_id(self)
        last_date = self.__get_last_date(table_id)
        if last_date is None:
            # Первый обмен каталога - выгружаем весь каталог, он уже содержит цены и остатки
            if not self.__export_catalog():
                return False
        else:
            # Обмен каталога уже был, грузим еще цены и остатки
            cnt_error = 0
            if not self.__export_catalog():
                cnt_error += 1
            if not self.__export_prices():
                cnt_error += 1
            if not self.__export_wares_rests():
                cnt_error += 1
            if cnt_error != 0:
                return False
        return True

    def _ping_catalog(self):
        """
        Отправка пустого каталога, для проверки можно ли что то отправлять
        @return: True в случае успеха
        """

        # Шапка
        sql_text = '''select datetimezone as dt from my_getdatetime(null,'DT')'''
        res = self.execute_sql(sql_text, fetch='one')
        today = res['datalist']['dt']
        xml = '<?xml version="1.0" encoding="UTF-8" ?><yml_catalog date="%s"><shop>' % today.strftime('%Y-%m-%d %H:%M')
        # Завершаем файл
        xml += '</yml_catalog>'

        response = self.__post_catalog('CatalogLoadingFromXml', xml)
        if response.status_code != 200:
            count = 1
            errors = ['Ошибка при изменении каталога товаров: ' + str(response.status_code) + ' ' + response.reason]
        else:
            # Сообщение может быть "Каталог успешно загружен на сервер"
            count, errors = self.check_errors(response.text)
        if count > 0:
            self.log_file('Ошибки при изменении каталога товаров: ', terms=1, save_log_db=True)
            for error in errors:
                self.log_file(error, terms=1, save_log_db=True)
        return count == 0

    def __export_catalog(self, not_ping=True):
        """
        Экспорт каталога
        @param not_ping: не отправлять пустой каталог
        :return: True, в случае успеха
        """
        table_id = Offer.fetch_table_id(self)
        last_date = self.__get_last_date(table_id)
        # Шапка
        sql_text = '''select datetimezone as dt from my_getdatetime(null,'DT')'''
        res = self.execute_sql(sql_text, fetch='one')
        today = res['datalist']['dt']
        # Валюты
        sql_text = '''select curid, symbol, '1' as rate from currency where curid=(select maincurid from config)'''
        currencies = Currency.gets(self.execute_sql, sql_text, ())
        # Организация
        sql_text = '''select s1.name as shop_name, s2.name as owner_name
                          from my_spobjects_get(null, null, ?) s1
                          left join my_spobjects_get(null, null, s1.higher) s2 on 1 = 1'''
        sql_params = [str(self.obj_id)]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')['datalist']
        obj = {
            'shop_name': res['shop_name'],
            'owner_name': res['owner_name']
        }
        # Категории
        categories = []
        if not_ping:
            sql_text = '''select g.waresgrid, g.name, g.higher from Q_API_GETWARESGROUPS(?,?,?,?,?,?) g'''
            sql_params = [self.obj_id, None, self.exchange_task_id, None, None, None]
            categories = Category.gets(self.execute_sql, sql_text, sql_params)

        # Товары
        offers = []
        if not_ping:
            # sql_text = 'select * from Q_API_GETGOODS(?,?,?,?,?,?)'
            sql_text = '''select g.waresid, g.name, g.pricesale, g.code, u.shortname as unit,
                          g.waresgrid, g.status,
                          p.filename_small as picture,
                          g.name as descript, g.rest,
                          trim(case g.status when '1' then 'true' else 'false' end) as available
                          from Q_API_GETGOODS(?,?,?,?,?,?) g
                          left join waresgroup wg on wg.waresgrid = g.waresgrid
                          left join API_GET_PHOTO(g.waresid, null) p on p.is_main = 1
                          left join gwares gw on gw.waresid = g.waresid
                          left join WARESUNIT wu on wu.waresunitid = gw.mainunitid
                          left join unit u on u.unitid = wu.unitid
                          order by g.rest desc
                          '''
            sql_params = [self.obj_id, None, self.exchange_task_id, None, last_date, None]
            offers = Offer.gets(self.execute_sql, sql_text, sql_params)
        if not self.update_catalog(today, currencies, obj, categories, offers):
            return False
        if not_ping:
            self.__exchange_success(table_id)
        return True

    def update_catalog(self, catalog_date, currencies, obj, categories, offers):
        if len(offers) == 0:
            return True
        # Начинаем файл
        xml = '<?xml version="1.0" encoding="UTF-8" ?><yml_catalog date="%s"><shop>' % catalog_date.strftime('%Y-%m-%d %H:%M')
        xml += '<name>%s</name><company>%s</company>' %\
               (YmlModel.yml_escape(obj['shop_name']), YmlModel.yml_escape(obj['owner_name']))
        # Валюты
        xml += '<currencies>'
        cur_id = None
        for item in currencies:
            if cur_id is None:
                cur_id = item.symbol
            xml += item.get_yml()
        xml += '</currencies>'
        # Категории
        xml += '<categories>'
        for item in categories:
            xml += item.get_yml()
        xml += '</categories>'
        # Товары
        xml += '<offers>'
        for item in offers:
            item.cur_id = cur_id
            if item.picture is not None:
                item.picture = self.sn_name + self.files_dir + '/' + self.layer_code + '/image/small/' + item.picture
            xml += item.get_yml()
        xml += '</offers>'
        # Завершаем файл
        xml += '</shop></yml_catalog>'
        response = self.__post_catalog('CatalogLoadingFromXml', xml)
        if response.status_code != 200:
            count = 1
            errors = ['Ошибка при изменении каталога товаров: ' + str(response.status_code) + ' ' + response.reason]
        else:
            # Сообщение может быть "Каталог успешно загружен на сервер"
            count, errors = self.check_errors(response.text)
        if count > 0:
            self.log_file('Ошибки при изменении каталога товаров: ', terms=1, save_log_db=True)
            for error in errors:
                self.log_file(error, terms=1, save_log_db=True)
        return count == 0

    def __export_prices(self):
        price_table_id = Price.fetch_table_id(self)
        last_date = self.__get_last_date(price_table_id)
        sql_text = '''select g.waresid, g.name, g.pricesale, g.code from q_api_getgoodspricesale(?,?,?,?,?,?) g'''
        sql_params = [self.obj_id, price_table_id, self.exchange_task_id, None, last_date, 'I']
        prices = Price.gets(self.execute_sql, sql_text, sql_params)
        if not self.update_prices(prices):
            return False
        self.__exchange_success(price_table_id)
        return True

    def update_prices(self, prices):
        if len(prices) == 0:
            return True
        xml = '<?xml version="1.0" encoding="UTF-8" ?><Root><GoodsCosts>'
        for item in prices:
            xml += item.get_yml()
        xml += '</GoodsCosts></Root>'
        response = self.__post('SetCostsForGoods', xml)
        if response.status_code != 200:
            count = 1
            errors = ['Ошибка при изменении цен товаров: ' + str(response.status_code) + ' ' + response.reason]
        else:
            count, errors = self.check_errors(response.text)
        if count > 0:
            self.log_file('Ошибки при изменении цен товаров: ', terms=1, save_log_db=True)
            for error in errors:
                self.log_file(error, terms=1, save_log_db=True)
        return count == 0

    def __export_wares_rests(self):
        """
        Экспорт товарных остатков
        :return: True, в случае успеха
        """
        table_id = Offer.fetch_table_id(self)
        rest_table_id = Rest.fetch_table_id(self)
        last_date = self.__get_last_date(rest_table_id)
        # Товарные остатки
        sql_text = 'select * from Q_API_GETGOODSREST(?,?,?,?,?,?)'
        flags = 'I'
        sql_params = [self.obj_id, table_id, self.exchange_task_id, None, last_date, flags]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='all')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при получении товарных остатков' + c.t_double_enter)
            return False
        else:
            rows = res['datalist']
            rests = []
            for row in rows:
                rest = Rest()
                rest.from_dict(row)
                rest.store = self.store_name
                rests.append(rest)
            if not self.update_wares_rests(rests):
                return False
        self.__exchange_success(rest_table_id)
        return True

    def update_wares_rests(self, rests):
        """
        Обновление остатков товара
        :param rests: list of orm.models.yml.Rest
        :return: boolean
        """
        if len(rests) == 0:
            return True
        xml = '<?xml version="1.0" encoding="UTF-8" ?><Root><GoodsOnStores>'
        for rest in rests:
            xml += rest.get_yml()
        xml += '</GoodsOnStores></Root>'
        response = self.__post('SetGoodsOnStores', xml)
        if response.status_code != 200:
            count = 1
            errors = ['Ошибка при изменении товарных остатков: ' + str(response.status_code) + ' ' + response.reason]
        else:
            count, errors = self.check_errors(response.text)
        if count > 0:
            self.log_file('Ошибки при изменении товарных остатков: ', terms=1, save_log_db=True)
            for error in errors:
                self.log_file(error, terms=1, save_log_db=True)
        return count == 0

    def check_errors(self, response_text):
        """
        <TagXXX><Errors><Error>...</Error></Errors></TagXXX>
        @param response_text:
        @return:
        """
        errors = []
        matches = re.findall(self.regex_errors, response_text)
        if matches is not None:
            for match in matches:
                errors.append(match.encode('cp1251'))
        return len(errors), errors

    @timer
    def __post(self, path, xml, multipart=True):
        """
        Реализация метода POST
        :param path: метод (путь), добавляемый к url API
        :param xml: XML в cp1251
        :param multipart: Отправлять XML как файл
        :return: requests.Response
        """
        xml = xml.decode('cp1251')
        self.text_save_to_file(xml.encode('utf-8'), '/' + str(self.exchange_task_id) + path + '.xml')
        if multipart:
            data = None
            files = {
                'file': ('data.xml', xml, 'text/xml'),
                'password': (None, self.password),
                'login': (None, self.login)
            }
        else:
            data = {
                'data': xml,
                'login': self.login,
                'password': self.password
            }
            files = None
        return requests.post(self.url + path, data=data, files=files, proxies=self.get_proxies())

    @timer
    def __post_catalog(self, path, xml, multipart=True):
        """
        Реализация метода POST
        :param path: метод (путь), добавляемый к url API
        :param xml: XML в cp1251
        :param multipart: Отправлять XML как файл
        :return: requests.Response
        """
        xml = xml.decode('cp1251')
        self.text_save_to_file(xml.encode('utf-8'), '/' + str(self.exchange_task_id) + path + '.xml')
        if multipart:
            data = None
            files = {
                'loadFileModel.files[0]': ('data.xml', xml, 'text/xml'),
                'loadFileModel.format': (None, '1'),
                'password': (None, self.password),
                'login': (None, self.login)
            }
        else:
            data = {
                'loadFileModel.files[0]': xml,
                'loadFileModel.format': '1',
                'login': self.login,
                'password': self.password
            }
            files = None
        return requests.post(self.url + path, data=data, files=files, proxies=self.get_proxies())

    def __get_last_date(self, table_id, update_exec_date=True, utc=False):
        """
        Получение даты/времени последнего обмена с интернет-магазином
        @param table_id: ID таблицы
        @param update_exec_date: Обновить дату/время запуска
        @param utc: Вернуть время в UTC
        @return: date
        """
        last_date = None
        sql_text = 'select lastdate, utclastdate from Q_API_GETEXCHANGETIMESTAMP(?,?,?,?,?)'
        sql_params = [table_id, None, self.exchange_task_id, None, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при получении даты последнего обмена с интернет-магазином' + str(self.exchange_task_id) + c.t_double_enter)
            return None
        else:
            row = res['datalist']
            if row is not None:
                if utc:
                    last_date = row['utclastdate']
                else:
                    last_date = row['lastdate']
        if update_exec_date:
            # Обновим
            sql_text = 'select * from Q_API_EXCHANGETASKSUCCESS(?,?,?,?,?,?)'
            sql_params = [table_id, None, self.exchange_task_id, None, self.queueid, 'S']
            res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка при обновлении даты последнего обмена с интернет-магазином ' + str(self.exchange_task_id) + c.t_double_enter)
        return last_date

    def __exchange_success(self, table_id):
        """
        Установка даты/времени последнего успешного обмена с интернет-магазином
        @param table_id: ID таблицы
        @return: date
        """
        # Обновим
        sql_text = 'select * from Q_API_EXCHANGETASKSUCCESS(?,?,?,?,?,?)'
        sql_params = [table_id, None, self.exchange_task_id, None, None, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при обновлении даты последнего успешного обмена с интернет-магазином ' + str(self.exchange_task_id) + c.t_double_enter)
        pass

    # todo Думаю метод стоит перенести, так как может понадобиться не только в yml
    @staticmethod
    def json_encode_1251(json_dict):
        """
        Преобразование ключей и значений dict к cp1251
        :param json_dict: входящий dict
        :return: преобразованный dict
        """
        res = {}
        if json is not None:
            for key, value in json_dict.items():
                key = key.encode('cp1251')
                if isinstance(value, six.string_types):
                    value = value.encode('cp1251')
                res[key] = value
        return res
