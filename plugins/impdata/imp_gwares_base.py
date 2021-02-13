# -*- coding: utf-8 -*-

"""
    Базовый модуль импорта товаров
"""

import krconst as c

VERSION = '0.0.3.0'
__author__ = 'swat'


class BaseIGwares(object):
    """
        Базовый класс импорта товаров
    """
    # parent_class - ссылка на класс который унаследован от BasePlugin, что бы иметь доступ
    # к необходимым методам. Или класс в котором есть методы execute_sql и log_file

    parent_class = None

    name = None
    code = None
    main_unit = None
    second_unit = None
    parent_code = None
    articul = None
    tax = None
    delete_marker = None
    expiration_type = None
    expiration_value = None
    parent = None
    external_id = None
    group_id = None
    factor = None

    wares_id = None

    def __init__(self):
        pass

    def save(self):
        sql_text = 'select * from RBS_Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [self.name, self.code, self.main_unit, self.parent_code, self.articul,
                      self.tax, self.delete_marker, 'I', self.expiration_type,
                      self.expiration_value, self.parent, None,
                      None, self.external_id, None, self.group_id, None]

        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='one')
        if res['status'] == c.kr_sql_error:
                self.parent_class.log_file(c.m_e_i_wares % self.code,
                                           terms=1)
        else:
            self.wares_id = res['datalist']['waresid']
