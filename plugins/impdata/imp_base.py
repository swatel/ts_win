# -*- coding: utf-8 -*-

"""
    модуль базовыми функциями для мипрта данных, документов
"""

from rbsqutils import str_to_bool_int

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '30.04.2015'


class ImpBase(object):
    """
        Класс базовыми функциями для импорта данных, документов
    """

    # json или xml объект, взависимости от того, какой класс унаследован
    obj = None
    encode = None

    def json_get_value(self, key):
        """
            Получение значения по ключу
        """

        try:
            result = self.obj[key]
        except KeyError:
            result = None
        if result:
            if self.encode:
                result = result.encode('cp1251')
        return result

    @staticmethod
    def str_to_bool_int(text):
        """
            Перенаправление
        """
        return str_to_bool_int(text)
