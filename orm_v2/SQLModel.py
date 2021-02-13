# -*- coding: utf-8 -*-
# todo нужно подумать как формировать лог файл. например метод from_json есть try. Но мы никогда не узнаем что у нас нет нужного ключа в json
import krconst as c
from orm_v2.SQLColumn import *


FETCH_NONE = 'none'
FETCH_ONE = 'one'
FETCH_MANY = 'many'


def _collect_attributes(cls, new_attrs, look_for_class):
    """
    Поиск по атрибутам `new_attrs` экземпларов класса `look_for_class` с удалением их из класса.
    Возвращает список атрибутов.
    @param cls: Класс, по которму искать
    @param new_attrs: Атрибуты
    @param look_for_class: Класс для поиска экземпляров
    @return:
    """
    result = []
    for attr, value in list(new_attrs.items()):
        if isinstance(value, look_for_class):
            delattr(cls, attr)
            result.append((attr, value))
    return result


def sql_execute_check(func):
    """
    Декоратор для получения результата выполнения запроса к БД
    @param func:
    @return:
    """
    def tmp(*args, **kwargs):
        _self = args[0]
        model = _self
        model.last_db_execute_status = None
        model.last_db_execute_error = None
        res = func(*args, **kwargs)
        model.last_db_execute_status = res['status']
        if model.last_db_execute_status == c.kr_sql_error:
            model.last_db_execute_error = res['error_db']
        return res
    return tmp


class DeclarativeMeta(type):
    """
    SQLObjects declarative
    """
    def __new__(mcs, class_name, bases, new_attrs):
        post_funcs = []
        early_funcs = []
        cls = type.__new__(mcs, class_name, bases, new_attrs)
        for func in early_funcs:
            func(cls)
        if '__classinit__' in new_attrs:
            cls.__classinit__ = staticmethod(cls.__classinit__.__func__)
        cls.__classinit__(cls, new_attrs)
        for func in post_funcs:
            func(cls)
        return cls


class SQLModel(object, metaclass=DeclarativeMeta):
    _table_name = None
    _pk = None
    _select_sql = None
    _save_sql = None
    _save_schema = None
    _alias = 't'
    _columns = None  # Имена колонок
    _real_columns = None  # Имена колонок, которые действительно есть в таблице, для построения авто запроса
    __columns_def = None  # Определение колонок
    last_db_execute_status = None
    last_db_execute_error = None
    converters = None

    def __classinit__(cls, new_attrs):
        # Колонки таблицы
        cls._columns = []
        cls._real_columns = []
        cls.__columns_def = dict()
        columns = _collect_attributes(cls, new_attrs, SQLColumn)
        for name, column in columns:
            cls._columns.append(name)
            cls.__columns_def[name] = column

    def __init__(self):
        """
        """
        # Создаем колонки из определений
        for name, column_def in self.__columns_def.items():
            column = column_def.new_instance()
            self.__dict__[name] = column
            if column.primary_key:
                self._pk = name
            if column.real_column:
                self._real_columns.append(name)

    def __getattribute__(self, item):
        columns = object.__getattribute__(self, '_columns')
        if item in columns:
            return self._get_column(item).__get__(self)
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        if key in self._columns:
            self._get_column(key).__set__(self, value)
        else:
            super(SQLModel, self).__setattr__(key, value)

    def save(self, execute_sql_func, sql_text=None, sql_params_schema=None, params=None, auto_commit=True):
        """

        @param execute_sql_func:
        @param sql_text:
        @param sql_params_schema:
        @param params: dict
        @param auto_commit:
        @return:
        """
        if sql_text is None:
            sql_text = self._get_save_sql()
        if sql_params_schema is None:
            sql_params_schema = self._save_schema
        sql_params = []
        # Параметры сопоставления параметров процедуры и модели
        for param_item in sql_params_schema:
            if param_item is None:
                sql_params.append(None)
            elif isinstance(param_item, str):
                # Если сторока - то значение поля с этим наименованием
                sql_params.append(self._get_value(param_item))
            elif isinstance(param_item, dict):
                # Настройки параметра
                if 'value' in param_item:
                    # По значению
                    sql_params.append(param_item['value'])
                elif 'param' in param_item:
                    # По имени параметра
                    if param_item['param'] in params:
                        sql_params.append(params[param_item['param']])
                    else:
                        sql_params.append(None)
        res = self.execute_sql(execute_sql_func, sql_text, sql_params=sql_params, fetch=FETCH_ONE,
                               auto_commit=auto_commit)
        status = self.last_db_execute_status
        if status == c.sql_ok:
            data = res['datalist']
            if data is None:
                print('Warning: save result is empty for model ' + self.__class__.__name__)
            elif self._pk is not None and self._get_column(self._pk).name in data and self._get_value(self._pk) is None:
                field = self._get_column(self._pk)
                field.__set__(self, data[self._get_column(self._pk).name])
        return status == c.sql_ok

    def __alias(self, column):
        """
        Получение наименования колонки с алиасом таблицы
        @param column: Имя колнки
        @return: str
        """
        return self._alias + '.' + self.__get_column_def(column).name

    def _get_save_sql(self):
        """
        Select запрос по умолчанию
        @return: str
        """
        if self._save_sql is None:
            raise NotImplementedError('Не указан SQL-запрос для сохранения ' + self._table_name)
        return self._save_sql

    def _get_select_sql(self):
        """
        Select запрос по умолчанию
        @return: str
        """
        if self._select_sql is None:
            sql = 'select '
            columns = list(map(self.__alias, self._real_columns))
            sql += ','.join(columns) + ' from ' + self._table_name + ' ' + self._alias
            if self._pk is not None:
                sql += ' where ' + self._alias + '.' + self._get_column(self._pk).name + '=?'
            self._select_sql = sql
        return self._select_sql

    def __get_column_def(self, name):
        """
        Поиск определения колонки
        @param name:
        @return:
        """
        if name in self._columns:
            return self.__columns_def[name]
        raise NameError('Column defination not found')

    def _get_column(self, name):
        """
        Колонка по имени
        @param name: Наименование
        @return: SQLField
        """
        if name in self._columns:
            return self.__dict__[name]
        raise NameError('Column %s not found' % name)

    def _get_value(self, name):
        """
        Значение по имени
        @param name: Наименование
        @return: значение
        """
        return self._get_column(name).__get__(self)

    def get_column_value(self, name):
        """
        Значение колонки по имени
        @param name: Наименование
        @return: значение
        """
        return self._get_value(name)

    @sql_execute_check
    def execute_sql(self, execute_sql_func, sql_text, sql_params=(), fetch=FETCH_MANY, auto_commit=True):
        """
        Запрос
        @param execute_sql_func:
        @param sql_params:
        @param sql_text:
        @param fetch:
        @param auto_commit:
        @return:
        """
        return execute_sql_func(sql_text, sql_params=sql_params, fetch=fetch, auto_commit=auto_commit)

    def select(self, execute_sql_func, sql_text, sql_params=(), fetch=FETCH_MANY):
        """
        Select запрос
        @param execute_sql_func:
        @param sql_params:
        @param sql_text:
        @param fetch:
        @param auto_commit:
        @return:
        """
        return self.execute_sql(execute_sql_func, sql_text, sql_params=sql_params, fetch=fetch)['datalist']

    def from_dict(self, source):
        """
        Загрузка значений из dict
        @param source: Источник
        @return:
        """
        if source is None:
            print('Warning: source dict is empty for model ' + self.__class__.__name__)
            return
        for name in self._columns:
            field = self._get_column(name)
            if field.name in source:
                field.__set__(self, source[field.name])
            else:
                field.__set__(self, None)
                print('Not in list:' + field.name)

    @classmethod
    def get(cls, pk_value, execute_sql_func, sql_text=None, params=None):
        """
        Создание модели с загрузкой данных из БД
        @param pk_value: Значение PK
        @param execute_sql_func: Функция выполнения sql
        @param sql_text:
        @param params:
        @return: SQLModel
        """
        model = cls()
        if sql_text is None:
            sql_text = model._get_select_sql()
            if model._pk is not None:
                params = [pk_value]
        else:
            if params is None:
                params = [pk_value]
        data = model.select(execute_sql_func, sql_text, sql_params=params, fetch=FETCH_ONE)
        # если метод получения данных из робота
        # todo Щеглов: необходимо дописать что бы метод в случае ошибки возвращал данные в плагин
        model.from_dict(data)
        return model

    @classmethod
    def gets(cls, execute_sql_func, sql_text, params):
        """
        Создание массива моделей с загрузкой данных из БД
        @param execute_sql_func: Функция выполнения sql
        @param sql_text:
        @param params:
        @return: список SQLModel
        """
        result = None
        model = cls()
        data = model.select(execute_sql_func, sql_text, sql_params=params, fetch=FETCH_MANY)
        if model.last_db_execute_status == c.sql_ok:
            result = []
            # если метод получения данных из робота
            # todo Щеглов: необходимо дописать что бы метод в случае ошибки возвращал данные в плагин
            for row in data:
                model = cls()
                model.from_dict(row)
                result.append(model)
        return result
