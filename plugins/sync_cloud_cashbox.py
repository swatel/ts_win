# -*- coding: utf-8 -*-


import sys
import os
import BasePlugin as Bp
from ext_api.q_exchange import QExchange
import krconst as c
import orm.models.cloud_cashbox.cc_equipment as equipment
import orm.models.cloud_cashbox.cc_object as object


class Plugin(Bp.BasePlugin, QExchange):
    """
    Синхронизация с внешним API
    """
    def run(self):

        self.p_c = self
        self.setup_load()

        if self.prepare_api():
            self.__process()

    def _exit(self, message=None, result=c.plugin_error):
        self.result['result'] = result
        if message is not None:
            self.log_file(message, terms=2)
        return False

    def prepare_api(self):

        # Имя модуля
        if 'ext_api' not in self.params:
            self.log_file('В параметрах задачи обмена не указано имя модуля')
            return False
        # Загрузка модуля
        module_name = self.params['ext_api']
        module = self.__import_module(module_name)
        if module is not None:
            try:
                self.__api = module.Api(self, self.params)
            except Exception as exc:
                self.log_file('Ошибка при загрузке модуля ' + module_name)
                self.log_file(str(exc))
                self.result['result'] = c.plugin_error
                return False
        return True

    def __process(self):
        """
        Обмена
        @return: 
        """

        if self.__api.supports('export_store'):
            self.__export_store()

        if self.__api.supports('import_store'):
            self.__import_store()

        if self.__api.supports('import_cashdesk'):
            self.__import_cashdesk()

        if self.__api.supports('export_cashdesk'):
            self.__export_cashdesk()

        # sync gwares
        if self.__api.supports('export_gwares'):
            self.__export_gwares()

        # sync docs
        if self.__api.supports('import_docs'):
            self.__import_doc()

    def __import_store(self):
        """
        Импорт магазинов
        @return: результат импорта
        """

        res = self.__api.get_store()

        params = {'exchange_task_id': self.exchange_task_id,
                  'flag': 'A',
                  'cashdesk_app_id': self.params['cashdesk_app_id']}
        for store in res:
            obj = object.Object(self.execute_sql)
            store['table_id'] = obj.table_id
            obj.from_dict(store)
            if not obj.save(self.execute_sql, params=params):
                self.log_file('Не удалось импортировать кассу')
                self.log_file(obj.last_db_execute_error)
                return False
        return True

    def __export_store(self):
        """
        Экспорт магазинов
        @return: результат экспорта
        """

        # получим магазины

        sql_text = 'select o.* from Q_API_GET_SHOPS o'
        sql_params = []
        r = self.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='many')
        if r['status'] == c.kr_sql_error:
            self.log_file('Не удалось получить магазины')
            return False

        for itm in r['datalist']:
            obj = object.Object.get(itm['OBJID'], self.execute_sql, params=[self.exchange_task_code])
            if obj.obj_id:
                res = self.__api.set_store(obj)
                if res:
                    self.external_inssel(res[0]['externalid'], obj.obj_id, table_name=obj._table_name)
            else:
                self.log_file(obj.last_db_execute_error)

        return True

    def __import_cashdesk(self):
        """
        Импорт касс
        @return: результат импорта
        """

        res = self.__api.get_cashdesk()
        params = {'exchange_task_id': self.exchange_task_id,
                  'flag': 'A',
                  'cashdesk_app_id': self.params['cashdesk_app_id']}
        for equp in res:
            eq = equipment.Eqipment(self.execute_sql)
            equp['table_id'] = eq.table_id
            eq.from_dict(equp)
            if not eq.save(self.execute_sql, params=params):
                self.log_file('Не удалось импортировать кассу')
                self.log_file(eq.last_db_execute_error)
                return False
        return True

    def __export_cashdesk(self):
        """
        Экспорт касс
        @return: результат экспорта
        """

        return True
            
    def __export_gwares(self):
        """
        Экспорт товаров
        @return: 
        """

        last_date = self.__get_last_date(None, 'GOODS')

        # проверим есть ли персональный метод для интеграции
        if self.__api.supports('export_method_gwares'):
            data = self.__api.export_method_gwares(last_date)
            res = self.__api.set_gwares(None, data)
            if res:
                self.__exchange_success(None, 'GOODS')
            # if res_dict:
            #     for itm in res_dict:
            #         self.external_inssel(itm['externalid'], itm['waresid'], table_name='GOODS')
            #     self.__exchange_success(None, 'GOODS')
        else:
            res = self.__api.get_cashdesk()
            shops = {}

            cashdesk = {}
            for i in res:
                sql_text = 'select qe.* from Q_EXTERNAL_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?) qe'
                sql_params = [None, None, 'equipment', None, self.exchange_task_code,
                    None, None, None, i['externalid'],None ,'Se', None]
                r = self.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
                if r['status'] == c.kr_sql_error:
                    self.log_file('Не удалось получить кассу')
                    # return False
                else:
                    cashdesk[r['datalist']['internalid']] = i['externalid']

            if not cashdesk:
                    self.log_file('Не удалось получить ни одной кассы')
                    return False

            sql_text = 'select eq.equipmentid,eq.ownerid,eq.serialnum from EQUIPMENT eq WHERE equipmentid in (%s)' % repr(list(cashdesk.keys()))[1:-1]
            r = self.execute_sql(sql_text=sql_text, fetch='many')
            if r['status'] == c.kr_sql_error:
                self.log_file('Не удалось получить магазины')
                return False

            for i in r['datalist']:
                if i['ownerid'] in shops:
                    shops[i['ownerid']]['cashdesk'].append(i['serialnum'])
                else:
                    shops[i['ownerid']] = {'cashdesk':[i['serialnum'],]}

            # Prepare wares and groups
            for i in shops:
                sql_text = "select mp.* from API_MYPOS_PRODUCTS(?,?,?,'S') mp"
                sql_params = [i, None]
                r = self.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='many')
                if r['status'] == c.kr_sql_error:
                    self.log_file('Не удалось получить товары и группы')
                    continue
                for cd in shops[i]['cashdesk']:
                    res_dict = self.__api.set_gwares(cd, r['datalist'])
                    if res_dict:
                        for itm in res_dict:
                            self.external_inssel(itm['externalid'], itm['waresid'], table_name='GOODS')

            pass

    def __import_doc(self):
        """
        Импорт документов
        @return: 
        """

        last_date = self.__get_last_date(None, 'MY_DOC')

        res = self.__api.get_cashdesk()
        shops = {}
        res = []
        res.append({'externalid': '381'})

        cashdesk = []
        for i in res:
            sql_text = 'select qe.*, e.equipmenthash ' \
                       '  from Q_EXTERNAL_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?) qe ' \
                       '       left join equipment e on qe.internalid = e.equipmentid'
            sql_params = [None, None, 'equipment', None, self.exchange_task_code,
                          None, None, None, i['externalid'], None, 'Se', None]
            r = self.execute_sql(sql_text=sql_text, sql_params=sql_params, fetch='one')
            if r['status'] == c.kr_sql_error:
                self.log_file('Не удалось получить кассу')
                # return False
            else:
                cashdesk.append(
                    {
                        'externalid': r['datalist']['externalid'],
                        'equipment_hash': r['datalist']['equipmenthash']
                    }
                )

        if not cashdesk:
            self.log_file('Не удалось получить ни одной кассы')
            return False

        if self.__api.get_docs(cashdesk, last_date):
            self.__exchange_success(None, 'MY_DOC')

    
    def external_inssel(self, external_id, internal_id, table_id=None, table_name=None):
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
        sql_params = [None, table_id, table_name,
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

    def __import_module(self, module):
        """
        Динамическая загрузка модуля Python
        :param module: путь к модулю
        :return: загруженный модуль
        """
        location = self.parent.k_conf.main_path + '/plugins/ext_api/' + module + '/' + module
        self.log_file('Загрузка модуля:', save_log_db=True)
        self.log_file(location, save_log_db=True)
        if not os.access(location + '.py', os.F_OK) and not os.access(location + '.pyc', os.F_OK):
            self.log_file('Not found plugin ' + location + '.py')
            return None
        (head, tail) = os.path.split(location)
        sys.path[0:0] = [head]
        result = __import__(tail)
        del sys.path[0]
        return result

    def __get_cashdesk(self):
        """
        Получение касс
        @return: 
        """

        pass

    def __get_last_date(self, table_id, table_name, update_exec_date=True, utc=False):
        """
        Получение даты/времени последнего обмена с интернет-магазином
        @param table_id: ID таблицы
        @param update_exec_date: Обновить дату/время запуска
        @param utc: Вернуть время в UTC
        @return: date
        """
        last_date = None
        sql_text = 'select cast(lastdate as Date) lastdate, utclastdate from Q_API_GETEXCHANGETIMESTAMP(?,?,?,?,?)'
        sql_params = [table_id, table_name, self.exchange_task_id, None, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при получении даты последнего обмена с ' + str(self.exchange_task_id) + c.t_double_enter)
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
            sql_params = [table_id, table_name, self.exchange_task_id, None, self.queueid, 'S']
            res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка при обновлении даты последнего обмена с ' + str(self.exchange_task_id) + c.t_double_enter)
        return last_date

    def __exchange_success(self, table_id, table_name):
        """
        Установка даты/времени последнего успешного обмена с интернет-магазином
        @param table_id: ID таблицы
        @return: date
        """
        # Обновим
        sql_text = 'select * from Q_API_EXCHANGETASKSUCCESS(?,?,?,?,?,?)'
        sql_params = [table_id, table_name, self.exchange_task_id, None, self.queueid, None]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при обновлении даты последнего успешного обмена с интернет-магазином ' + str(self.exchange_task_id) + c.t_double_enter)