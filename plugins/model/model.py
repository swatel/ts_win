# -*- coding: utf-8 -*-

import krconst as c

VERSION = '0.0.1.1'


class Model(object):
    """
        Базовый класс моделей
    """
    _parent_obj = None  # BasePlugin
    _attributes = []  # Поля таблицы
    table_name = None  # Таблица
    pk_name = 'id'  # Первичный ключ
    code_name = None  # Поле код (если есть)
    # Для обмена
    _table_id = None
    _external_id = None
    _external_code = None

    @property
    def table_id(self):
        if self._table_id is None:
            self._table_id = self.fetch_table_id(self._parent_obj)
        return self._table_id

    # @table_id.setter
    # def table_id(self, table_id):
    #     if table_id is not None:
    #         table_id = int(table_id)
    #     self._table_id = table_id

    @property
    def external_id(self):
        return self._external_id

    @external_id.setter
    def external_id(self, external_id):
        if external_id is not None:
            external_id = int(external_id)
        self._external_id = external_id

    @property
    def external_code(self):
        return self._external_code

    @external_code.setter
    def external_code(self, external_code):
        self._external_code = external_code

    def __init__(self, parent_obj):
        self._parent_obj = parent_obj

    @classmethod
    def dict(cls, parent_obj, source={}):
        """
        Создание экземпляра класса с загрузкой его атрибутов из dict
        :param parent_obj: базовый плагин BasePlugin
        :param source: dict значений
        :return: модель, экземпляр класса
        """
        model = cls(parent_obj)
        for name, attr in model._attributes.items():
            if name in source:
                model.__setattr__(name, source[name])
            else:
                model.__setattr__(name, None)
        return model

    @classmethod
    def dict_ex(cls, parent_obj, source={}):
        """
        Создание экземпляра класса с загрузкой его атрибутов из dict + внешних идентификаторов
        :param parent_obj: базовый плагин BasePlugin
        :param source: dict значений
        :return: модель, экземпляр класса
        """
        model = cls.dict(parent_obj, source)
        model.external_code = source['externalcode']
        model.external_id = source['externalid']
        return model

    @classmethod
    def fetch_table_id(cls, parent_obj):
        """
        Получение tableid из таблицы r_table для текущей модели
        :param parent_obj: BasePlugin
        :return: int TABLEID
        """
        sql = 'select TABLEID as id from R_TABLE WHERE DBNAME=UPPER(?)'
        sql_params = [cls.table_name]
        res = parent_obj.execute_sql(sql, sql_params=sql_params, fetch='one')
        if res['status'] == c.kr_sql_error:
            parent_obj.log_file('Не удалось получить TABLEID таблицы ' + cls.table_name)
            return None
        else:
            return res['datalist']['id']

    def save(self):
        """
        Сохранение данных в БД, реализовать в потомках!
        :return:
        """
        raise Exception('Метод save не реализован!')

    def __call_external(self, table_id=None, exchange_task_id=None, exchange_task_code=None, queue_id=None, flags=''):
        """
        Вызов процедуры Q_EXTERNAL_INSSEL
        :param table_id: таблица для связи
        :param exchange_task_id: настройки связи
        :param exchange_task_code: код настроек связи
        :param queue_id: задача робота
        :param flags: действия Q_EXTERNAL_INSSEL
        :return: результат процедуы Q_EXTERNAL_INSSEL
        """
        # Значение PK
        if self.pk_name is not None and len(self.pk_name) > 0:
            internal_id = self.__getattribute__(self.pk_name)
            # Подготовим под типы данных процедуры
            if internal_id is not None:
                internal_id = str(internal_id)
        else:
            internal_id = None
        # Значение кода
        if self.code_name is not None and len(self.code_name) > 0:
            internal_code = self.__getattribute__(self.code_name)
        else:
            internal_code = None
        # Подготовим под типы данных процедуры
        if self._external_id is not None:
            external_id = str(self._external_id)
        else:
            external_id = None
        if self._external_code is not None:
            external_code = str(self._external_code)
        else:
            external_code = None
        # Флаги процедуры Q_EXTERNAL_INSSEL
        # flags = 'SIie' - сохранение (по умолчанию)
        # flags = 'Se' - проверка существования externalid
        # Сохраняем
        sql = 'select * from Q_EXTERNAL_INSSEL(?,?,?,?,?,?,?,?,?,?,?)'
        params = [None, table_id, self.table_name, exchange_task_id, exchange_task_code,
                  internal_code, internal_id, external_code, external_id, queue_id, flags]
        # Нужно будет получить результат
        res = self._parent_obj.execute_sql(sql, sql_params=params, fetch='one')
        if res['status'] == c.kr_sql_error:
            # self._parent_obj.log_file(c.m_e_u_wgroup_external % self.internal_id + c.t_double_enter)
            self._parent_obj.log_file('Ошибка сохранения внешнего id ' + str(internal_id) + c.t_enter + res['message'])
            return None
        else:
            return res['datalist']
            
    def store_external(self, table_id=None, exchange_task_id=None, exchange_task_code=None, queue_id=None):
        """
        Сохранение связи внутренних идентификаторов с внешними
        :param table_id: таблица для связи
        :param exchange_task_id: настройки связи
        :param exchange_task_code: код настроек связи
        :param queue_id: задача робота
        :return: результат процедуы Q_EXTERNAL_INSSEL
        """
        return self.__call_external(table_id, exchange_task_id, exchange_task_code, queue_id, flags='SIie')
        
    def check_external(self, table_id=None, exchange_task_id=None, exchange_task_code=None, queue_id=None):
        """
        Проверка существования связи внутренних идентификаторов с внешними
        :param table_id: таблица для связи
        :param exchange_task_id: настройки связи
        :param exchange_task_code: код настроек связи
        :param queue_id: задача робота
        :return: True если связь существует
        """
        ret = self.__call_external(table_id, exchange_task_id, exchange_task_code, queue_id, flags='Se')
        if ret is not None:
            return ret['id'] is not None
        return None

