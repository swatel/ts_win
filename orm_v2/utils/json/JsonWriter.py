# -*- coding: utf-8 -*-
import json
from orm_v2.SQLColumnDef import *
from rbsqutils import decodeUStr


class JsonWriter(object):
    __converter = None

    def __get_column_name(self, name):
        if self.__converter is not None and name in self.__converter:
            return self.__converter[name]
        return name

    def __get_value_for_json(self, name, fk_as_object=False, execute_sql_func=None):
        """
        Значение по имени (для json)
        @param name: Наименование
        @return: Значение
        """
        column = self._get_column(name)
        value = self._get_value(name)
        if fk_as_object and column.foreign_key is not None:
            if value is None:
                return None
            return column.fk_model(execute_sql_func).get_json(column_prefix=column.json_column_prefix)
        else:
            if isinstance(column, TimestampColumnDef):
                return str(value)
            return value

    def json_write(self, encoding='cp1251', prefix='', fk_as_object=False, execute_sql_func=None, converter='json',
                   decode_u_str=True):
        """
        Преобразование модели в json
        @param encoding: Кодировка
        @param prefix: Префикс для имен в JSON
        @param fk_as_object: Выгружать внешние ключи как вложенный объект
        @param execute_sql_func: Функция выполнения SQL
        @param converter: Конвертер имен колонок в JSON
        @return: json
        """
        self.__converter = None
        if converter is not None and converter in self.converters:
            self.__converter = self.converters[converter]

        json_obj = dict([(prefix + self.__get_column_name(name),
                                          self.__get_value_for_json(name,
                                                                    fk_as_object=fk_as_object,
                                                                    execute_sql_func=execute_sql_func)
                                          ) for name in self._columns])
        if encoding == 'cp1251':
            if decode_u_str:
                return decodeUStr(json.dumps(json_obj, encoding='cp1251', indent=1))
            return json.dumps(json_obj, encoding='cp1251', indent=1)
        return json.dumps(json_obj, encoding='cp1251', indent=1).encode(encoding)
