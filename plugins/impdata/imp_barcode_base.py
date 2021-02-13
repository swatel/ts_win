# -*- coding: utf-8 -*-

"""
    Базовый модуль импорта ШК товара
"""

import krconst as c

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '17.07.2015'


class BaseBarcode(object):
    """
        базовый класс импорта ШК товара
    """

    parent_class = None

    code = None
    unit = None
    barcode = None
    factor = None
    uweight = None
    ulength = None
    uheight = None
    uwidth = None
    coef_ttx = 1
    external_id = None

    def __init__(self):
        """
            Инициализация
        """

        pass

    def save(self):
        """
            сохраним ШК
        """

        sql_text = 'select * from RBS_Q_IMP_WARESBARCODE(?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [self.code, self.unit, self.barcode, self.factor,
                      self.uweight, self.ulength, self.uheight, self.uwidth,
                      self.coef_ttx, None, self.external_id]
        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='one')
        if res['status'] == c.kr_sql_error:
            message = c.m_e_i_wares % self.code + '. ' + c.m_e_i_wares_barcode % self.barcode
            self.parent_class.LogFile(message + c.t_double_enter)
