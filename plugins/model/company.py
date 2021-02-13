# -*- coding: utf-8 -*-

import model as md
# import krconst as c

VERSION = '0.0.1.1'


class Company(md.Model):
    """
        Реализация компании
    """
    table_name = 'COMPANY'

    def __init__(self, parent_obj):
        super(Company, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = 'compid'
        # self.code_name = 'code'
        # Допустимые атрибуты
        self._attributes = {
            'compid': {},
            'name': {},
            'email': {},
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
        return True
