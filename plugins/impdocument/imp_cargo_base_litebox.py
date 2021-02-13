# -*- coding: utf-8 -*-

"""
    Базовый модуль импорта позиций документа
"""

import krconst as c

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '21.07.2015'


class BaseCargoLB(object):
    """
        Базовый класс импорта позиций документа
    """

    parent_class = None

    doc_id = None
    action_status = None
    sum_with_nds = None
    code_wares = None
    external_id = None
    name_wares = None
    amount = None
    price = None
    doc_sum = None
    code_unit = None
    external_tax_code = None

    def save(self):
        """
            сохраним позицию документа
        """

        sql_text = 'execute procedure Q_IMP_CARGO_LB(?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [self.doc_id, self.action_status, self.sum_with_nds, self.code_wares,
                      self.external_id, self.name_wares, self.amount, self.price, self.doc_sum,
                      self.code_unit, self.external_tax_code]
        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='one')
        if res['status'] == c.kr_sql_error:
            message = 'Ошибка сохранения позиции документа'
            self.parent_class.LogFile(message + c.t_double_enter)
