# -*- coding: utf-8 -*-

import model as md
import krconst as c

VERSION = '0.0.1.1'


class Assortment(md.Model):
    """
        Реализация товарных групп
    """
    table_name = 'gwares'

    def __init__(self, parent_obj):
        super(Assortment, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = 'assortmentid'
        self.code_name = ''
        # Допустимые атрибуты
        self._attributes = {
            'assortmentid': {},
            'objid': {},
            'waresid': {},
            'saleprice': {},
            'salerestrict': {},
            'fromplace': {},
        }
        for name, attr in self._attributes.items():
            self.__setattr__(name, None)

    def save(self):
        """
        Сохранение данных в БД
        :return:
        """
        # sql = 'select * ' \
        #       'from MY_ASSORTMENT_SET((SELECT assortmentid from assortment where objid=? and waresid=?),?,?,?,?,?,?,?,?,?,?,?,(SELECT p.uid FROM MY_TASKSERVER_USER p),?,?,?)'
        sql = 'select * ' \
              'from MY_ASSORTMENT_SET((SELECT assortmentid from assortment where objid=? and waresid=?),?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)'
        params = [self.objid, self.waresid, self.objid, self.waresid, None, None, self.salerestrict, None, None, None,
                  self.saleprice, None, None, None, None, self.fromplace]
        res = self._parent_obj.execute_sql(sql, sql_params=params, fetch='one')
        if res['status'] == c.kr_sql_error:
            self._parent_obj.log_file('Ошибка при сохранении в БД ' + c.t_enter + res['message'] + c.t_enter)
            return False
        else:
            self.assortmentid = res['datalist']['assortmentid']
        return True
