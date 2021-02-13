# -*- coding: utf-8 -*-
import BasePlugin as Bp
import krconst as c
import json
from plugins.model.waresgroup import WaresGroup
from plugins.model.gwares import Gwares
from plugins.model.waresrest import Waresrest
from plugins.model.tax import Tax
from plugins.model.assortment import Assortment
from plugins.model.document import Document
from plugins.model.documentgood import Docgood
from plugins.model.company import Company
import time
import os
import sys
import six


class Plugin(Bp.BasePlugin):
    """

    """
    __api = None
    exchange_task_code = None
    exchange_task_id = None
    obj_id = None
    user_id = None

    def run(self):
        # Шаг 1: получить задачу обмена из параметров задания
        self.exchange_task_id = None
        if self.queueparamsxml:
            try:
                self.exchange_task_id = int(self.parser_xml(self.queueparamsxml, 'exchange_task_id'))
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

    def __process(self, params):
        """
        Процесс обмена
        :param params: параметры обмена
        :return: истина, если успешно
        """
        res = True
        # Имя модуля
        if 'ext_api' not in params:
            self.log_file('В параметрах задачи обмена не указано имя модуля')
            return False
        # Загрузка модуля
        module_name = params['ext_api']
        module = self.__import_module(module_name)
        if module is not None:
            try:
                self.__api = module.Api(self, params)
            except:
                self.log_file('Ошибка при загрузке модуля ' + module_name)
                return False
            # Определяем направление обмена
            if 'parent' in params and params['parent'] is not None and params['parent'] == '1':
                parent = True
            else:
                parent = False
            # Если родительская система - та, с которой обмен
            if parent:
                # Загружаем из родительской системы (LiteBox дочерняя система)
                res = self.__import()
            else:
                # Иначе выгружаем в дочернюю систему (LiteBox материнская система)
                res = self.__export()
            if res and self.__api.supports('orders'):
                res = self.doc_exchange()
        return res

    def external_inssel(self, external_id, internal_id, table_id):
        """

        @param external_id: Внешний идентификатор
        @param internal_id: Внутренний иденитификатор
        @param table_id: Таблица
        @return: Boolean
        """
        if isinstance(internal_id, float):
            # DocID
            internal_id = int(internal_id)
        sql_text = 'select * from Q_EXTERNAL_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [None, table_id, None,
                      self.exchange_task_id, None, None,
                      str(internal_id), None, str(external_id),
                      None, 'iI', None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка создания связи с внешней системой', save_log_db=True)
            return False
        return True

    def find_by_external_id(self, table_id, external_id):
        """
        Поиск внутреннего идентификатора по внешнему
        @param table_id: Таблица
        @param external_id: Внешний ИД
        @return: Внутренний ИД
        """
        sql_text = 'select internalid from Q_EXTERNAL_INSSEL(?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [None, table_id, None,
                      self.exchange_task_id, None, None,
                      None, None, str(external_id),
                      None, 'eS']
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка поиска документа заказа', save_log_db=True)
            return None
        elif res['datalist'] is not None:
            return res['datalist']['internalid']
        else:
            return None

    def find_doc(self, external_id=None, doc_id=None, doc_bond=None):
        """
        Поиск документа по идентификатору (внешнему или внутреннему) + связи
        @param external_id: Внешний ИД Документа
        @param doc_id: ИД Документа
        @param doc_bond: Связь
        @return: Boolean, ИД связанного документа и статус
        """
        sql_text = 'select * from Q_API_EXCHANGE_FIND_DOC(?,?,?,?,?,?)'
        sql_params = [doc_id, str(external_id), self.exchange_task_id,
                      None, doc_bond, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка поиска связанного документа', save_log_db=True)
            return False, None, None
        else:
            return True, res['datalist']['docid'], res['datalist']['status']

    def doc_up(self, doc_id):
        """
        Поднятие статуса документа
        @param doc_id: ИД документа
        @return: Boolean
        """
        sql_text = 'execute procedure MY_REALIZAT_DOC_STATUS_UP_DOWN(?,?,?,?,?,?)'
        sql_params = [doc_id, 'up', None,
                      self.user_id, None, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='none')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка поиска типа документа заказ', save_log_db=True)
            return False
        return True

    def get_client(self, client):
        """
        Получение клиента магазина
        @param client: Объект
        @return: ObjID
        """
        table_id = Company.fetch_table_id(self)
        if client.external_id is not None:
            external_id = client.external_id
        else:
            external_id = client.email
        obj_id = self.find_by_external_id(table_id, external_id)
        if obj_id is None:
            sql_text = '''select objid
                          from my_spobjects_edit(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            sql_params = [client.name, None, 'f', None, None, None, None, None, None, None,
                          client.email, None, None, None, None, None, '0', None, '0', None,
                          '0', None, None, '1', None, None, None, None, None, '1',
                          None]
            res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка создания клиента', save_log_db=True)
                return False, None
            else:
                obj_id = res['datalist']['objid']
            sql_text = '''execute procedure
                          my_spobjects_edit_category(?,(select catid from CATEGORY where code='CUSTOMER'),?,?)'''
            sql_params = [obj_id, '1', self.user_id]
            res = self.execute_sql(sql_text, sql_params=sql_params, fetch='none')
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка добавления клиента в категорию', save_log_db=True)
                return False, None
            if not self.external_inssel(external_id, obj_id, table_id):
                return False, None
        return True, obj_id

    def doc_exchange(self):
        """
        Обмен документами
        @return: Boolean
        """
        table_id = Document.fetch_table_id(self)
        goods_table_id = Gwares.fetch_table_id(self)
        # По last_date ищем в базе Лайтбокса
        last_date = self.__get_last_date(table_id)
        # По UTC ищем в Эквиде
        utc_last_date = self.__get_last_date(table_id, utc=True)
        timestamp = None
        if utc_last_date is not None:
            timestamp = int(time.mktime(utc_last_date.timetuple()))
        # Забираем из Эквида
        res, orders = self.__api.get_orders(timestamp)
        if res:
            sql_text = '''select c.maincurid, d.doctid, ds.doctid as sdoctid, dr.doctid as rdoctid,
                          (SELECT p.uid FROM MY_TASKSERVER_USER p) as userid
                          from config c
                          left join doctype d on d.code = 'ORDER'
                          left join doctype ds on ds.code = 'SALE'
                          left join doctype dr on dr.code = 'RET'
            '''
            res = self.execute_sql(sql_text, fetch='one')
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка поиска типа документа заказ', save_log_db=True)
                return False
            else:
                doctype = res['datalist']['doctid']
                doctype_sale = res['datalist']['sdoctid']
                doctype_ret = res['datalist']['rdoctid']
                currency = res['datalist']['maincurid']
                self.user_id = res['datalist']['userid']
            for order in orders:
                # Может в будущем и пригодится процедура MY_REALIZATION_GET_DOCS_FILTER, пока не вижу в ней смысла
                res, order_doc_id, status = self.find_doc(external_id=order.external_id)
                if res:
                    order.docid = order_doc_id
                    if order.client is not None:
                        res, fromobj = self.get_client(order.client)
                        if res:
                            order.fromobj = fromobj
                        else:
                            return False
                    if order.docid is None:
                        # Создаем новый заказ
                        sql_text = 'select * from MY_REALIZATION_CREATE_DOCUMENT(?,?,?,?,?,?,?,?)'
                        sql_params = [order.fromobj, self.obj_id, order.docdate.strftime('%d.%m.%Y %H:%M:%S'),
                                      doctype, currency, None, self.user_id, None]
                        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
                        if res['status'] == c.kr_sql_error:
                            self.log_file('Ошибка создания документа заказа', save_log_db=True)
                            return False
                        else:
                            order.docid = res['datalist']['docid']
                            if not self.external_inssel(order.external_id, order.docid, table_id):
                                return False
                    if order.docid is not None and order.goods is not None and status != 'v':
                        # Если есть товарные позиции и документ можно редактировать
                        for cargo in order.goods:
                            # Поиск товара по внешнему идентификатору
                            cargo.waresid = self.find_by_external_id(goods_table_id, cargo.w_external_id)
                            if cargo.waresid is not None:
                                cargo.waresid = int(cargo.waresid)
                            else:
                                if self.__import_single_wares(cargo):
                                    cargo.waresid = self.find_by_external_id(goods_table_id, cargo.w_external_id)
                                    if cargo.waresid is not None:
                                        cargo.waresid = int(cargo.waresid)
                                else:
                                    self.log_file('Ошибка определения позиции заказа', save_log_db=True)
                                    return False
                            sql_text = 'execute procedure MY_REALIZATION_EDIT_GOODS(?,?,?,?,?,?,?)'
                            sql_params = [order.docid, cargo.waresid, cargo.amount,
                                          cargo.price, None, self.user_id, None]
                            res = self.execute_sql(sql_text, sql_params=sql_params, fetch='none')
                            if res['status'] == c.kr_sql_error:
                                self.log_file('Ошибка сохранения позиции заказа', save_log_db=True)
                                return False
                    if order.external_status in ['AWAITING_PAYMENT', 'PAID'] and status != 'v':
                        # Для Заказа и Продажи процедуру Поднять статус вызвать два раза
                        if (status is None or status == '0') and not self.doc_up(order.docid):
                            return False
                        if not self.doc_up(order.docid):
                            return False
                    sale_doc_id = None
                    if order.external_status in ['REFUNDED', 'PAID']:
                        # Ищем продажу
                        res, sale_doc_id, status = self.find_doc(doc_id=order.docid, doc_bond='ORDERSALE')
                        if res:
                            if sale_doc_id is None:
                                status = '0'
                                # Добавляем продажу
                                sql_text = 'select * from MY_REALIZATION_CREATE_DOCUMENT(?,?,?,?,?,?,?,?)'
                                sql_params = [self.obj_id, order.fromobj, order.docdate.strftime('%d.%m.%Y %H:%M:%S'),
                                              doctype_sale, currency, None, self.user_id, int(order.docid)]
                                res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
                                if res['status'] == c.kr_sql_error:
                                    self.log_file('Ошибка создания документа продажи', save_log_db=True)
                                    return False
                                else:
                                    sale_doc_id = res['datalist']['docid']
                                    # if not self.external_inssel(order.external_id, sale_doc_id, table_id):
                                    #     return False
                            if sale_doc_id is not None and status != 'v':
                                # Поднимаем статус
                                # Для Заказа и Продажи процедуру Поднять статус вызвать два раза
                                if (status is None or status == '0') and not self.doc_up(sale_doc_id):
                                    return False
                                if not self.doc_up(sale_doc_id):
                                    return False
                        else:
                            return False
                        # Добавить оплату, ждем #2058 - у нас нет факта оплаты.
                    if order.external_status in ['REFUNDED']:
                        # Ищем возврат
                        res, ret_doc_id, status = self.find_doc(doc_id=order.docid, doc_bond='SALERETBND')
                        if res:
                            if ret_doc_id is None:
                                status = '0'
                                # Добавляем возврат
                                sql_text = 'select * from MY_REALIZATION_CREATE_DOCUMENT(?,?,?,?,?,?,?,?)'
                                sql_params = [order.fromobj, self.obj_id, order.docdate.strftime('%d.%m.%Y %H:%M:%S'),
                                              doctype_ret, currency, None, self.user_id, int(sale_doc_id)]
                                res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
                                if res['status'] == c.kr_sql_error:
                                    self.log_file('Ошибка создания документа возврата', save_log_db=True)
                                    return False
                                else:
                                    ret_doc_id = res['datalist']['docid']
                                    # if not self.external_inssel(order.external_id, ret_doc_id, table_id):
                                    #     return False
                            if ret_doc_id is not None and status != 'v':
                                # Поднимаем статус
                                # Для Возврата от покупателя процедуру Поднять статус вызвать один раз
                                if not self.doc_up(ret_doc_id):
                                    return False
                else:
                    return False
            # Выгружаем в ecwid
            sql_text = '''select * from Q_API_GETDOCS(?,?,?,?,?,?)'''
            sql_params = [self.obj_id, table_id, self.exchange_task_id,
                          None, last_date, None]
            res = self.execute_sql(sql_text, sql_params=sql_params)
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения документов из Litebox', save_log_db=True)
                return False
            else:
                rows = res['datalist']
                for row in rows:
                    document = Document(self)
                    document.docid = row['docid']
                    document.external_id = row['externalid']
                    document.amount = row['amount']
                    document.status = row['status']
                    client = Company(self)
                    client.email = row['clientemail']
                    if row['clientexternalid'] is not None:
                        try:
                            client.external_id = row['clientexternalid']
                        except ValueError:
                            client.external_id = None
                    document.external_paid = row['paid']
                    document.external_refunded = row['refunded']
                    document.client = client
                    # Получение товарных позиций
                    sql_text = '''select * from Q_API_GETDOCGOODS(?,?,?,?,?,?)'''
                    sql_params = [document.docid, self.obj_id, self.exchange_task_id,
                                  None, goods_table_id, None]
                    if self.__check_wares_lb(sql_params):
                        cargo_res = self.execute_sql(sql_text, sql_params=sql_params)
                        if cargo_res['status'] == c.kr_sql_error:
                            self.log_file('Ошибка получения товарных позиций документа из Litebox', save_log_db=True)
                            return False
                        else:
                            cargo_rows = cargo_res['datalist']
                            goods = []
                            for cargo_row in cargo_rows:
                                cargo = Docgood(self)
                                cargo.waresid = cargo_row['waresid']
                                cargo.name = cargo_row['waresname']
                                cargo.w_external_id = cargo_row['waresexternalid']
                                cargo.amount = cargo_row['waresamount']
                                cargo.price = cargo_row['waressaleprice']
                                goods.append(cargo)
                            document.goods = goods
                            res = document.external_id is not None
                            if res:
                                res, ecwid_order = self.__api.get_doc(document.external_id)
                                if not res:
                                    return False
                            else:
                                ecwid_order = None
                            if ecwid_order is None:
                                # Документа нет, создаем новый
                                res = self.__api.create_order(document)
                                if res:
                                    document.store_external(table_id=table_id,
                                                            exchange_task_id=self.exchange_task_id)
                                else:
                                    return False
                            else:
                                # TODO Проверяем суммы и статусы?
                                res = self.__api.update_order(document)
                                if not res:
                                    return False
            self.__exchange_success(table_id)
            return True
        return False

    def __check_wares_lb(self, sql_params):
        """
        Провека товаров, что есть привязка к внешней системе
        @param sql_params:
        @return: Результат проверки
        """

        sql_text = '''select * from Q_API_GETDOCGOODS(?,?,?,?,?,?)'''
        cargo_res = self.execute_sql(sql_text, sql_params=sql_params)
        if cargo_res['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения товарных позиций документа из Litebox', save_log_db=True)
            return False
        else:
            cargo_rows = cargo_res['datalist']
            for cargo_row in cargo_rows:
                if not cargo_row['waresexternalid']:
                    if self.__api.supports('export_wares_groups') and \
                            not self.__export_wares_groups(waresid=cargo_row['waresid']):
                        return False
                    if self.__api.supports('export_gwares') and \
                            not self.__export_gwares(waresid=cargo_row['waresid']):
                        return False
            return True

    def __import_single_wares(self, cargo):
        """
        Импорт одиночного товара из внешней системы
        @param cargo: Товарная позиция документа
        @return: результат
        """

        if not self.__import_gwares(diff_only=False, external_id=cargo.w_external_id):
            return False
        return True

    def __import(self, diff_only=True):
        if not self.__import_taxes(diff_only):
            return False
        if not self.__import_wares_groups(diff_only):
            return False
        if not self.__import_gwares(diff_only):
            return False
        # if not self.__import_wares_rests():
        #     return False
        return True

    def __export(self, diff_only=True):
        """
        Экспорт в дочернюю систему
        :return: True в случае успеха
        """
        if self.__api.supports('export_profile') and not self.__export_profile():
            return False
        if self.__api.supports('export_taxes') and not self.__export_taxes(diff_only):
            return False
        if self.__api.supports('export_wares_groups') and not self.__export_wares_groups():
            return False
        if self.__api.supports('export_gwares') and not self.__export_gwares():
            return False
        if self.__api.supports('export_wares_rests') and not self.__export_wares_rests():
            return False
        return True

    def __export_profile(self):
        """
        Экспорт профиля магазина
        Пока заточен под !Ecwid!
        :return: True, в случае успеха
        """
        profile = {}
        # Информация о магазине
        sql_text = 'select fullname from GETOBJECTNAME(?,?)'
        sql_params = [self.obj_id, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Не удалось получить данные владельца магазина')
            return False
        profile['company_name'] = res['datalist']['fullname']
        # Экспорт
        self.__api.update_profile(profile)
        return True

    def __export_taxes(self, diff_only=True):
        """
        Экспорт налоговых ставок и стран, где они действуют
        @param diff_only: Только для не имеющих привязку к внешней системе
        :return: True, в случае успеха
        """
        table_id = Tax.fetch_table_id(self)
        last_date = self.__get_last_date(table_id)
        sql_text = 'select * from Q_API_GETTAXES(?,?,?,?,?,?)'
        sql_params = [self.obj_id, table_id, self.exchange_task_id, None, last_date, 'M']
        res = self.execute_sql(sql_text, sql_params=sql_params)
        if res['status'] == c.kr_sql_error:
            self.log_file('Не удалось получить данные налоговых ставок')
            return False
        # Экспорт
        taxes = []
        for row in res['datalist']:
            # todo Щеглов: Скорее всего не нужно, коментирую
            # if diff_only and row['externalid'] is not None:
            #         continue
            tax = Tax.dict(parent_obj=self, source=row)
            tax.countrycode = row['countrycode']
            tax.countryname = row['countryname']
            tax.external_code = row['externalcode']
            tax.external_id = row['externalid']
            taxes.append(tax)
        if self.__api.update_taxes(taxes):
            self.__exchange_success(table_id)
            return True
        else:
            return False

    def __import_taxes(self, diff_only=True):
        """
        Импорт налоговых ставок и стран, где они действуют
        @param diff_only: только для не имеющих привязки к внешней системе
        :return: True, в случае успеха
        """
        taxes = self.__api.get_taxes()
        if taxes:
            table_id = Tax.fetch_table_id(self)
            # last_date = self.__get_last_date(table_id)
            self.__get_last_date(table_id, utc=True)
            for tax in taxes:
                # todo Щеглов: Скорее всего не нужно, коментирую
                # if diff_only:
                #     if tax.check_external(table_id=table_id, exchange_task_id=self.exchange_task_id, queue_id=self.queueid):
                #         # связь существует - пропускаем
                #         continue
                if tax.save():
                    ext = tax.store_external(table_id=table_id, exchange_task_id=self.exchange_task_id, queue_id=self.queueid)
                    if ext is None:
                        return False
                else:
                    return False
            self.__exchange_success(table_id)
            return True
        else:
            return False

    def __import_wares_groups(self, diff_only=True, external_id=None):
        """
        Импорт Товарных групп
        @param diff_only: только для не имеющих привязки к внешней системе
        @param external_id: ид группы по внешней системе
        :return: True, в случае успеха
        """
        table_id = WaresGroup.fetch_table_id(self)
        last_date = self.__get_last_date(table_id, utc=True)
        groups = self.__api.get_wares_groups(last_date, external_id)
        if groups:
            for group in groups:
                if diff_only:
                    if group.check_external(table_id=table_id, exchange_task_id=self.exchange_task_id, queue_id=self.queueid):
                        # связь существует - пропускаем
                        continue
                if group.save():
                    ext = group.store_external(table_id=table_id, exchange_task_id=self.exchange_task_id, queue_id=self.queueid)
                    if ext is None:
                        return False
                else:
                    return False
            self.__exchange_success(table_id)
            return True
        else:
            if groups == False:
                return False
            else:
                return True

    def __import_gwares(self, diff_only=True, external_id=None):
        """
        Импорт товаров
        @param diff_only: только для не имеющих привязки к внешней системе
        @param external_id: ид товара по внешней системе
        :return: True, в случае успеха
        """
        table_id = Gwares.fetch_table_id(self)
        last_date = self.__get_last_date(table_id, utc=True)
        tax = self.__get_object_tax()
        if tax is not None:
            external_tax = tax['externalcode']
        else:
            external_tax = None
        gwares = self.__api.get_gwares(last_date, external_id)
        if isinstance(gwares, list):
            # Может быть
            for wares in gwares:
                if external_id:
                    if wares.wg_external_id != 0:
                        self.__import_wares_groups(diff_only=True, external_id=wares.wg_external_id)
                wares.external_tax = external_tax
                # todo Щеглов: Скорее всего не нужно, коментирую
                # if diff_only:
                #     if wares.check_external(table_id=table_id, exchange_task_id=self.exchange_task_id, queue_id=self.queueid):
                #         # связь существует - пропускаем
                #         continue
                if wares.save():
                    ext = wares.store_external(table_id=table_id, exchange_task_id=self.exchange_task_id, queue_id=self.queueid)
                    if ext is None:
                        return False
                    # Сохранить цену
                    assortment = Assortment(self)
                    assortment.waresid = wares.waresid
                    assortment.saleprice = wares.pricesale
                    assortment.objid = self.obj_id
                    assortment.fromplace = 'SYNCAPI'  # TODO передавать имя обмена, а не плагина. Пока не критично.
                    if not assortment.save():
                        return False
                else:
                    return False
            self.__exchange_success(table_id)
            return True
        else:
            return False

    def __get_object_tax(self):
        """
        Получение параметров налога по умолчанию
        @return:
        """
        sql_text = 'select * from Q_API_GETOBJECTTAX(?,?)'
        sql_params = [self.obj_id, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file(c.m_e_g_wgroup_tree + c.t_double_enter)
            return None
        else:
            return res['datalist']

    def __export_wares_groups(self, waresid=None):
        """
        Экспорт товарных групп
        @param waresid: Для единичного экспорта. Групп по товару
        :return: True, в случае успеха
        """
        table_id = WaresGroup.fetch_table_id(self)
        last_date = None
        if not waresid:
            last_date = self.__get_last_date(table_id)
        # Товарные группы
        sql_text = 'select * from Q_API_GETWARESGROUPS(?,?,?,?,?,?,?)'
        sql_params = [self.obj_id, table_id, self.exchange_task_id, None, last_date, None, waresid]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='all')
        if res['status'] == c.kr_sql_error:
            self.log_file(c.m_e_g_wgroup_tree + c.t_double_enter)
            return False
        else:
            parents = {}  # Для промежуточного хранения пар Наш ID: ID Ecwid
            rows = res['datalist']
            for row in rows:
                group = WaresGroup.dict_ex(parent_obj=self, source=row)
                group.parent = None  # Идентификатор внешнего родителя
                """if str(row['deleted']) == '1':
                    # Удаляем
                    if group.external_id is not None:
                        self.__api.delete_wares_group(group)
                elif"""
                if group.external_id is not None:
                    # Изменяем
                    if group.higher is not None and group.higher in parents:
                        group.parent = parents[group.higher]
                    if self.__api.update_wares_group(group):
                        parents[group.waresgrid] = group.external_id
                    else:
                        return False
                else:
                    # Добавляем
                    if group.higher is not None and group.higher in parents:
                        group.parent = parents[group.higher]
                    if self.__api.add_wares_group(group):
                        ext = group.store_external(table_id, exchange_task_code=self.exchange_task_code,
                                                   exchange_task_id=self.exchange_task_id, queue_id=self.queueid)
                        if ext is None:
                            return False
                        parents[group.waresgrid] = group.external_id
                    else:
                        return False
        self.__exchange_success(table_id)
        return True

    def __export_gwares(self, waresid=None):
        """
        Экспорт товаров
        @param waresid: Для единичного экспорта
        :return: True, в случае успеха
        """
        table_id = Gwares.fetch_table_id(self)
        last_date = None
        if not waresid:
            last_date = self.__get_last_date(table_id)
        # Товары
        sql_text = 'select * from Q_API_GETGOODS(?,?,?,?,?,?,?)'
        sql_params = [self.obj_id, table_id, self.exchange_task_id, None, last_date, None, waresid]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='all')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при получении товаров' + c.t_double_enter)
            return False
        else:
            rows = res['datalist']
            if self.__api.supports('export_gwares', 'batch'):
                goodss = []
                for row in rows:
                    goodss.append(Gwares.dict_ex(parent_obj=self, source=row))
                if not self.__api.update_gwares(goodss):
                    return False
            else:
                for row in rows:
                    goods = Gwares.dict_ex(parent_obj=self, source=row)
                    goods.wexternal_code = row['wgexternalcode']
                    goods.wexternal_id = row['wgexternalid']
                    """if str(row['deleted']) == '1':
                        # Удаляем
                        if goods.external_id is not None:
                            self.__api.delete_goods(goods)
                    el"""
                    if goods.external_id is not None:
                        # Изменяем
                        if not self.__api.update_goods(goods):
                            return False
                    else:
                        # Добавляем
                        if self.__api.add_goods(goods):
                            ext = goods.store_external(table_id, exchange_task_code=self.exchange_task_code,
                                                       exchange_task_id=self.exchange_task_id, queue_id=self.queueid)
                            if ext is None:
                                return False
                        else:
                            return False
        self.__exchange_success(table_id)
        return True

    def __export_wares_rests(self):
        """
        Экспорт товарных остатков
        @param diffonly: только не имеющие привязку к внешней системе
        :return: True, в случае успеха
        """
        table_id = Gwares.fetch_table_id(self)
        rest_table_id = Waresrest.fetch_table_id(self)
        last_date = self.__get_last_date(rest_table_id)
        # Товарные остатки
        sql_text = 'select * from Q_API_GETGOODSREST(?,?,?,?,?,?)'
        if self.__api.supports('flags'):
            flags = self.__api.flags('export_wares_rests')
        else:
            flags = None
        sql_params = [self.obj_id, table_id, self.exchange_task_id, None, last_date, flags]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='all')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при получении товарных остатков' + c.t_double_enter)
            return False
        else:
            rows = res['datalist']
            if self.__api.supports('export_wares_rests', 'batch'):
                rests = []
                for row in rows:
                    rests.append(Waresrest.dict_ex(parent_obj=self, source=row))
                if not self.__api.update_wares_rests(rests):
                    return False
            else:
                for row in rows:
                    rest = Waresrest.dict_ex(parent_obj=self, source=row)
                    if rest.external_id is not None:
                        # Изменяем
                        if not self.__api.update_wares_rest(rest):
                            return False
                    else:
                        return False
        self.__exchange_success(rest_table_id)
        return True

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

    def __import_module(self, module):
        """
        Динамическая загрузка модуля Python
        :param module: путь к модулю
        :return: загруженный модуль
        """
        location = self.parent.k_conf.main_path + '/plugins/ext_api/' + module + '/api'
        if not os.access(location + '.py', os.F_OK) and not os.access(location + '.pyc', os.F_OK):
            self.log_file('Not found plugin ' + location + '.py')
            return None
        (head, tail) = os.path.split(location)
        sys.path[0:0] = [head]
        result = __import__(tail)
        del sys.path[0]
        return result
