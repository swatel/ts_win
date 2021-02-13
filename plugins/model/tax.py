# -*- coding: utf-8 -*-

import model as md
import krconst as c

VERSION = '0.0.1.1'


class Tax(md.Model):
    """
        Реализация товарных групп
    """
    table_name = 'tax'

    def __init__(self, parent_obj):
        super(Tax, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = 'taxid'
        self.code_name = ''
        # Допустимые атрибуты
        self._attributes = {
            'taxid': {},
            'name': {},
            'rate': {},
            'countryid': {},
        }
        for name, attr in self._attributes.items():
            self.__setattr__(name, None)

    def save(self):
        """
        Сохранение данных в БД
        :return:
        """
        sql = 'select taxid from RBS_Q_TAX_INSSEL(?,?,?,?)'
        params = [str(self.external_code), self.name, self.rate, 'I']
        res = self._parent_obj.execute_sql(sql, sql_params=params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self._parent_obj.log_file('Ошибка при сохранении в БД ' + c.t_enter + res['message'])
            return False
        else:
            self.taxid = res['datalist']['taxid']
        return True
