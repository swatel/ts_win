# -*- coding: utf-8 -*-


"""
    модуль с базовыми функциями для обмена через Q_EXCHANGE
"""

import json
from rbsqutils import json_encode_1251

import krconst as c

__author__ = 'swat'
VERSION = '1.0.1.9'
DATE_VERSION = '09.02.2017'


class QExchange(object):
    """
            Базовый класс обмена
    """
    # parent_class - ссылка на класс который унаследован от BasePlugin, что бы иметь доступ
    # к необходимым методам. Или класс в котором есть методы execute_sql и log_file

    # parent_class
    p_c = None

    exchange_task_code = None
    exchange_task_id = None
    obj_id = None
    user_id = None
    params = {}

    def __init__(self):
        pass

    def setup_load(self):
        """
        Загрузка настроек обмена из БД
        """

        self.exchange_task_code = None
        self.exchange_task_id = None
        self.obj_id = None
        self.user_id = None
        self.params = {}

        # Шаг 1: получить задачу обмена из параметров задания
        if self.p_c.queueparamsxml:
            try:
                self.exchange_task_id = int(self.p_c.parser_xml(self.p_c.queueparamsxml, 'exchange_task_id'))
            except ValueError:
                self.exchange_task_id = None
        if self.exchange_task_id is None:
            self.p_c.log_file('Не указана задача обмена в параметрах очереди заданий', save_log_db=True)
            return False

        # Шаг 2: получить параметрамы обмена
        sql_text = 'select * from Q_API_GETEXCHANGETASK(?,?,?)'
        sql_params = [self.exchange_task_id, None, None]
        res = self.p_c.execute_sql(sql_text, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self.p_c.log_file('Ошибка получения параметров задачи обмена', save_log_db=True)
            return False
        else:
            row = res['datalist']
            if row['status'] != '1':
                self.p_c.log_file('Задача обмена заблокирована', save_log_db=True)
                return False
            # Владелец магазина
            self.obj_id = row['objid']
            self.exchange_task_code = row['code']
            # Шаг 3: склеить параметры
            self.params = {}
            if row['exchangeparams'] is not None:
                try:
                    self.params = json.loads(str(row['exchangeparams']), encoding='cp1251')
                except:
                    pass
            if row['params'] is not None:
                task_params = json.loads(str(row['params']), encoding='cp1251')
                for key, value in task_params.items():
                    self.params[key] = value
            self.params['exchangecode'] = row['exchangecode']
            # если есть параметр какое ПО стоит на кассе, то найдем его ид
            if self.params.get('cashdesk_app', None):
                sql_text = 'SELECT ca.cashdesk_app_id FROM cashdesk_apps ca WHERE ca.code = ?'
                sql_params = [(self.params['cashdesk_app']).encode('cp1251')]
                res = self.p_c.execute_sql(sql_text, sql_params=sql_params, fetch='one')
                if res['status'] == c.kr_sql_error:
                    self.p_c.log_file('Ошибка получения установленного на кассе ПО', save_log_db=True)
                else:
                    self.params['cashdesk_app_id'] = res['datalist']['cashdesk_app_id']
            else:
                self.params['cashdesk_app_id'] = None
            # Подготовим параметры для передачи дальше
            self.params = json_encode_1251(self.params)

        return True
