# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    класс импорта рецептов
"""

import krconst as k

from rbsqutils import str_to_bool_int


class Recipe():
    """
        класс импорта рецептов
    """

    _parent_class = None

    _external_code = None
    _wares_name = None
    _wares_code = None
    _name = None
    _quantity = None
    _delete_marker = None
    _date = None

    recipe_id = None
    result_class = k.plugin_ok

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self._parent_class = parent_class

        if obj is not None:
            self._external_code = self._xml_get_value(obj, 'id1c', flag='N')
            self._wares_name = self._xml_get_value(obj, 'waresname', flag='N')
            self._wares_code = self._xml_get_value(obj, 'warescode', flag='N')
            self._name = self._xml_get_value(obj, 'name', flag='N')
            self._quantity = self._xml_get_value(obj, 'quantity', flag='N')
            self._date = self._xml_get_value(obj, 'date', flag='N')
            self._delete_marker = self._xml_get_value(obj, 'deletemarker', flag='N')
            self._delete_marker = str_to_bool_int(self._delete_marker)

    def save(self):
        """
            сохраним рецепт
        """

        sql_text = 'select * from RBS_Q_RECIPE_INSEL (?,?,?,?,?,?,?)'
        sql_params = [self._external_code, self._wares_code, self._wares_name,
                      self._name, self._quantity, self._delete_marker, self._date]

        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams = sql_params,
                                            fetch='one',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_recipe % self._wares_code

            self._parent_class.log_file(message,
                                        terms=2,
                                        save_log_db=True)
            self.result_class = k.plugin_error
        else:
            ''' если рецепт удаленный, то не импортируем его компоненты
                то есть не возвращаем ид рецепта
            '''
            if self._delete_marker == '0':
                self.recipe_id = res['datalist']['RECIPEID']

    def _xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self._parent_class.xml_get_value_by_attr(xml, attr, flag)