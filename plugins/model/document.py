# -*- coding: utf-8 -*-

import model as md
# import krconst as c

VERSION = '0.0.1.1'


class Document(md.Model):
    """
        Реализация документа
    """
    table_name = 'MY_DOC'

    def __init__(self, parent_obj):
        super(Document, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = 'docid'
        # self.code_name = 'code'
        # Допустимые атрибуты
        self._attributes = {
            'docid': {},
            'docdate': {},
            'doctype': {},
            'docnumber': {},
            'goods': {},
            'fromobj': {},
            'client': {},
            'status': {},
            'amount': {},
            'external_status': {},
            'external_paid': {},
            'external_refunded': {}
        }
        for name, attr in self._attributes.items():
            self.__setattr__(name, None)

    def save(self):
        """
        Сохранение данных в БД
        :return:
        """
        if self.external_code is None:
            external_code = None
        else:
            external_code = str(self.external_code)
        if self.external_id is None:
            external_id = None
        else:
            external_id = str(self.external_id)
        # sql = 'select waresid from RBS_Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        # # params = [self.name, external_code, unit, wg_external_code, None, tax, None, 'I', None, None, None, None, None,
        # #           external_id, None, wg_external_id, None]
        # params = [self.name, None, unit, wg_external_code, None, tax, None, 'I', None, None, None, None, None,
        #           external_id, None, wg_external_id, None]
        # res = self._parent_obj.execute_sql(sql, sql_params=params, fetch='one')
        # if res['status'] == c.kr_sql_error:
        #     self._parent_obj.log_file('Ошибка при сохранении в БД ' + c.t_enter + res['message'] + c.t_enter)
        #     return False
        # else:
        #     self.waresid = res['datalist']['waresid']
        return True
