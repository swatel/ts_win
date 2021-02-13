# -*- coding: utf-8 -*-

"""
    модуль импорта ШК товара из json
"""

import plugins.impdata.imp_barcode_base as w
import plugins.impdata.imp_base as i_base

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '17.07.2015'


class BarcodeJson(w.BaseBarcode, i_base.ImpBase):
    """
        Класс  импорта ШК товара из json
    """

    obj = None

    def __init__(self, parent_class, obj, encode=True):
        """
            Инициализация переменных из json
        """

        self.parent_class = parent_class
        self.obj = obj
        self.encode = encode

        self.code = self.json_get_value('code')
        self.unit = self.json_get_value('main_unit_code')
        self.barcode = self.json_get_value('barcode')
        self.factor = self.json_get_value('factor')
        #self.uweight = None
        #self.ulength = None
        #self.uheight = None
        #self.uwidth = None
        #self.coef_ttx = 1
        self.external_id = self.json_get_value('guid')
