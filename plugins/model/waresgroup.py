# -*- coding: utf-8 -*-

import model as md
import krconst as c

VERSION = '0.0.1.1'


class WaresGroup(md.Model):
    """
        Реализация товарных групп
    """
    table_name = 'waresgroup'

    def __init__(self, parent_obj):
        super(WaresGroup, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = 'waresgrid'
        self.code_name = 'code'
        # Допустимые атрибуты
        self._attributes = {
            'waresgrid': {},
            'name': {},
            'higher': {},
            'num': {},
            'code': {},
            'status': {},
        }
        for name, attr in self._attributes.items():
            self.__setattr__(name, None)

    def save(self):
        """
        Сохранение данных в БД
        :return:
        """
        if hasattr(self, 'h_external_id') and self.h_external_id is not None:
            h_external_id = str(self.h_external_id)
        else:
            h_external_id = None
        if hasattr(self, 'h_external_code') and self.h_external_code is not None:
            h_external_code = str(self.h_external_code)
        else:
            h_external_code = None
        if self.external_code is None:
            external_code = None
        else:
            external_code = str(self.external_code)
        if self.external_id is None:
            external_id = None
        else:
            external_id = str(self.external_id)
        # TODO При работах над обменом исправить - externalid пишется в саму таблицу, а не в таблицу обмена!
        sql = 'select waresgrid from RBS_Q_WARESGROUP_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?)'
        params = [self.name, external_code, h_external_code, 'I', None, None, external_id, h_external_id,
                  None, None, None, None]
        res = self._parent_obj.execute_sql(sql, sql_params=params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self._parent_obj.log_file('Ошибка при сохранении в БД ' + c.t_enter + res['message'] + c.t_enter)
            return False
        else:
            self.waresgrid = res['datalist']['waresgrid']
        return True
