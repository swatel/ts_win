# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    модуль импорта групп товаров
"""

import krconst as k
from rbsqutils import str_to_bool_int


class WGroup():
    """
        Класс  импорта групп товаров
    """

    _parent_class = None

    _name = None
    _code = None
    _parent_code = None
    _delete_marker = None
    _parent = None
    _external_id = None
    _group_id = None

    wgroup_id = None

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self._parent_class = parent_class
        if obj is not None:
            self._code = self._xml_get_value(obj, 'warescode', flag='N')
            self._name = self._xml_get_value(obj, 'waresname', flag='N')
            self._delete_marker = self._xml_get_value(obj, 'deletemarker')
            self._delete_marker = str_to_bool_int(self._delete_marker)
            self._parent = self._xml_get_value(obj, 'parent', flag='N')
            self._parent_code = self._xml_get_value(obj, 'parentcode', flag='N')

            if parent_class.xml_name_external_id:
                self._external_id = self._xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
                self._group_id = self._xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')

            if self._parent_code == '0':
                self._parent_code = None

            ''' для поддержки старых форматов '''
            if not self._code:
                self._code = self._xml_get_value(obj, 'code', flag='N')
            if not self._name:
                self._name = self._xml_get_value(obj, 'name', flag='N')

    def save(self):
        """
            сохраним группу
        """

        self.wgroup_id = None

        sql_text = 'select * from RBS_Q_WARESGROUP_INSSEL(?,?,?,?,?,?,?,?)'
        sql_params = [self._name, self._code, self._parent_code, 'I', self._delete_marker,
                      self._parent, self._external_id, self._group_id]

        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams = sql_params,
                                            fetch='one',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            self._parent_class.LogFile(k.m_e_i_wgroup % self._code + k.kr_term_double_enter)
        else:
            self.wgroup_id = res['datalist']['WARESGRID']

    def _xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self._parent_class.xml_get_value_by_attr(xml, attr, flag)