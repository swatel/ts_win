# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    класс импорта должностей
"""

import krconst as k


class Dolgn():
    """
        Класс импорта должностей
    """

    parent_class = None

    dolgnid = None
    dolgnguid = None
    dolgncode = None
    dolgnname = None
    result_class = k.plugin_ok

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
            если obj=None, то переменные нужно заполнять вручную
        """

        self.parent_class = parent_class

        if obj is not None:
            self.dolgnguid = self._xml_get_value(obj, 'dolgnguid', flag='N')
            self.dolgncode = self._xml_get_value(obj, 'dolgncode', flag='N')
            self.dolgnname = self._xml_get_value(obj, 'dolgnname', flag='N')

    def save(self):
        """
            сохраним должность и получим должность
        """

        if self.dolgnguid:
            sql_text = 'select * from RBS_Q_DOLGN_INSSEL (?, ?, ?)'
            sql_params = [self.dolgnname, self.dolgnguid, self.dolgncode]

            res = self.parent_class.ExecuteSQL(sql_text,
                                               sqlparams = sql_params,
                                               fetch='one',
                                               ExtVer=True)
            if res['status'] == k.kr_sql_error:
                message = k.m_w_importdolgn % self.dolgncode

                self.parent_class.log_file(message,
                                           terms=2,
                                           save_log_db=True)
                self.result_class = k.plugin_error
            else:
                self.dolgnid = res['datalist']['DOLGNID']

    def _xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self.parent_class.xml_get_value_by_attr(xml, attr, flag)

