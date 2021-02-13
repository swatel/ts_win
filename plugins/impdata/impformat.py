# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    модуль импорта справочника форматов
"""

import krconst as k


class Format():
    """
        класс импорта справочника форматов
    """

    _parent_class = None

    _name = None

    result_class = k.plugin_ok

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self._parent_class = parent_class

        if obj is not None:
            self._name = self._xml_get_value(obj, 'name', flag='N')

    def save(self):
        """
            сохраним рецепт
        """

        sql_text = 'execute procedure RBS_Q_FORMAT_INSSEL(?)'
        sql_params = [self._name]

        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams = sql_params,
                                            fetch='one',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_format % self._name

            self._parent_class.log_file(message,
                                        terms=2,
                                        save_log_db=True)
            self.result_class = k.plugin_error

    def _xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self._parent_class.xml_get_value_by_attr(xml, attr, flag)