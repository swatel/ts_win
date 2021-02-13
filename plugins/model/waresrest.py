# -*- coding: utf-8 -*-

import model as md

VERSION = '0.0.1.1'


class Waresrest(md.Model):
    """
        Реализация товарных групп
    """
    table_name = 'waresrest'

    def __init__(self, parent_obj):
        super(Waresrest, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = ''
        self.code_name = ''
        # Допустимые атрибуты
        self._attributes = {
            'waresid': {},
            'name': {},
            'code': {},
            'waresunitid': {},
            'famount': {}
        }
