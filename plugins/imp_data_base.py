# -*- coding: utf-8 -*-

"""
    модуль импорта данных (объектов, товаров) из внешних систем
    базовый модуль
"""

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '08.07.2015'


class ImpData(object):
    """
        Базовый класс
    """

    p_c = None

    def __init__(self, parent_class):
        """
            Инициализация
            @param parent_class: ссылка на класс который унаследован от BasePlugin, что бы иметь доступ
            к необходимым методам
        """
