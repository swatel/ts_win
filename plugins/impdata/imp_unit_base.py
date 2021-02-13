# -*- coding: utf-8 -*-

"""
    Базовый модуль импорта ед измерения
"""

import krconst as c

VERSION = '0.0.3.0'
__author__ = 'swat'


class UnitBase(object):
    """
        Базовый класс импорта ед измерения
    """

    # parent_class - ссылка на класс который унаследован от BasePlugin,
    # что бы иметь доступ к необходимым методам. Или класс в котором
    # есть методы execute_sql и log_file
    parent_class = None

    external_id = None
    external_code = None
    short_name = None
    full_name = None
    factor = None

    unit_id = None

    def __init__(self):
        """
            Иницализация
        """

        pass

    def save(self):
        """
            сохраним ед измерения
        """

        sql_text = 'select * from RBS_Q_UNIT_INSSEL(?,?,?,?,?,?)'
        sql_params = [self.external_code, self.full_name, self.short_name,
                      self.factor, 'I', self.external_id]

        res = self.parent_class.ExecuteSQL(sql_text,
                                           sqlparams=sql_params,
                                           fetch='one',
                                           ExtVer=True)
        if res['status'] == c.kr_sql_error:
            message = c.m_e_i_unit % self.external_code + c.t_double_enter
            self.parent_class.log_file(message,
                                       save_log_db=True)
        else:
            self.unit_id = res['datalist']['UNITID']
