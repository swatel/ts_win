# -*- coding: utf-8 -*-

import model as md
import krconst as c

VERSION = '0.0.1.1'


class Gwares(md.Model):
    """
        Реализация товарных групп
    """
    table_name = 'gwares'

    def __init__(self, parent_obj):
        super(Gwares, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = 'waresid'
        self.code_name = 'code'
        # Допустимые атрибуты
        self._attributes = {
            'waresid': {},
            'code': {},
            'name': {},
            'waresgroup': {},
            'pricesale': {},
            'status': {},
            'picture': {}
        }
        for name, attr in self._attributes.items():
            self.__setattr__(name, None)

    def save(self):
        """
        Сохранение данных в БД
        :return:
        """
        if hasattr(self, 'wg_external_id') and self.wg_external_id is not None:
            wg_external_id = str(self.wg_external_id)
        else:
            wg_external_id = None
        if hasattr(self, 'wg_external_code') and self.wg_external_code is not None:
            wg_external_code = str(self.wg_external_code)
        else:
            wg_external_code = None
        if self.external_code is None:
            external_code = None
        else:
            external_code = str(self.external_code)
        if self.external_id is None:
            external_id = None
        else:
            external_id = str(self.external_id)
        if hasattr(self, 'unit') and self.unit is None:
            unit = None
        else:
            unit = str(self.unit)
        if hasattr(self, 'external_tax') and self.external_tax is None:
            tax = None
        else:
            tax = str(self.external_tax)
        # TODO При работах над обменом исправить - externalid пишется в саму таблицу, а не в таблицу обмена!
        sql = 'select waresid from RBS_Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        # params = [self.name, external_code, unit, wg_external_code, None, tax, None, 'I', None, None, None, None, None,
        #           external_id, None, wg_external_id, None]
        params = [self.name, None, unit, wg_external_code, None, tax, None, 'I', None, None, None, None, None,
                  external_id, None, wg_external_id, None]
        res = self._parent_obj.execute_sql(sql, sql_params=params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self._parent_obj.log_file('Ошибка при сохранении в БД ' + c.t_enter + res['message'] + c.t_enter)
            return False
        else:
            self.waresid = res['datalist']['waresid']
        return True
