# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    модуль импорта ед измерения
"""

import krconst as k


class Unit():
    """
        класс импорта рецептов
    """

    _parent_class = None

    _external_id = None
    _external_code = None
    _short_name = None
    _full_name = None
    _factor = None

    unit_id = None

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self._parent_class = None
        self._external_id = None
        self._external_code = None
        self._short_name = None
        self._full_name = None
        self._factor = None

        self._parent_class = parent_class
        if obj is not None:
            if parent_class.xml_name_external_id:
                self._external_id = self._xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
            self._external_code = self._xml_get_value(obj, 'code', flag='N')
            self._short_name = self._xml_get_value(obj, parent_class.xml_unit_short_name, flag='N')
            self._full_name = self._xml_get_value(obj, parent_class.xml_unit_full_name, flag='N')
            if parent_class.unit_flag_factor == 'EXT':
                self._factor = self._xml_get_value(obj, 'factor', flag='N')
            else:
                self._factor = None

    def save(self):
        """
            сохраним ед измерения
        """

        sql_text = 'select * from RBS_Q_UNIT_INSSEL(?,?,?,?,?,?)'
        sql_params = [self._external_code, self._full_name, self._short_name, self._factor, 'I', self._external_id]

        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams = sql_params,
                                            fetch='one',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            self._parent_class.LogFile(k.m_e_i_unit % self._external_code + k.kr_term_double_enter)
        else:
            self.unit_id = res['datalist']['UNITID']

    def _xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self._parent_class.xml_get_value_by_attr(xml, attr, flag)