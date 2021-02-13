# -*- coding: utf-8 -*-

import model as md
# import krconst as c

VERSION = '0.0.1.1'


class Docgood(md.Model):
    """
        Реализация документа
    """
    table_name = 'MY_DOC_GOODS'

    def __init__(self, parent_obj):
        super(Docgood, self).__init__(parent_obj=parent_obj)
        # Переопределение первичного ключа
        self.pk_name = 'cargoid'
        # self.code_name = 'code'
        # Допустимые атрибуты
        self._attributes = {
            'cargoid': {},
            'price': {},
            'amount': {},
            'waresid': {},
            'name': {},
            'w_external_id': {}
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
