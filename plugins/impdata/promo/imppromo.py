# -*- coding: utf-8 -*-
"""
    swat 08.08.2014
    version 0.0.2.0
    модуль импорта промо акций
"""

import krconst as k


class Promo():
    """
        Класс  импорта промо акций
    """

    _parent_class = None

    __obj = None

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self._parent_class = parent_class
        self.__obj = obj
