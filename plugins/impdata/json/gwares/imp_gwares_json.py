# -*- coding: utf-8 -*-

"""
    модуль импорта товаров из json
"""

import plugins.impdata.imp_gwares_base as w
import plugins.impdata.imp_base as i_base

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '16.07.2015'


class GwaresJson(w.BaseIGwares, i_base.ImpBase):
    """
        Класс  импорта товаров из json
    """

    obj = None

    def __init__(self, parent_class, obj, encode=True):
        """
            Инициализация переменных из json
        """

        self.parent_class = parent_class
        self.obj = obj
        self.encode = encode

        self.name = self.json_get_value('name')
        self.code = self.json_get_value('code')
        self.main_unit = self.json_get_value('main_unit_code')

        #parent_code = None
        self.articul = self.json_get_value('articul')
        self.tax = self.json_get_value('tax_name')
        self.delete_marker = self.json_get_value('is_delete')
        if self.delete_marker == '0':
            self.delete_marker = '1'
        elif self.delete_marker == '1':
            self.delete_marker = '0'
        #expiration_type = None
        #expiration_value = None
        #parent = None
        self.external_id = self.json_get_value('guid')
        self.group_id = self.json_get_value('group_guid')
        self.factor =  self.json_get_value('factor')

