# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    класс импорта компонентов для рецепта
"""

import krconst as k


class Component():
    """
        класс импорта рецептов
    """

    _parent_class = None

    _wares_name = None
    _wares_code = None
    _wares_unit = None
    _quantity = None

    result_class = k.plugin_ok

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self._parent_class = parent_class

        if obj is not None:
            self._wares_name = self._xml_get_value(obj, 'waresname', flag='N')
            self._wares_code = self._xml_get_value(obj, 'warescode', flag='N')
            self._wares_unit = self._xml_get_value(obj, 'unit', flag='N')
            self._quantity = self._xml_get_value(obj, 'quantity', flag='N')

    def save(self, recipe_id):
        """
            сохраним компоненты рецепта
        """

        sql_text = 'execute procedure RBS_Q_COMPONENT_INSEL (?, ?, ?, ?, ?)'
        sql_params = [recipe_id, self._wares_code, self._wares_name,
                      self._wares_unit, self._quantity]

        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams = sql_params,
                                            fetch='none',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_component % self._wares_code

            self._parent_class.log_file(message,
                                        terms=2,
                                        save_log_db=True)
            self.result_class = k.plugin_error

    def clear(self, recipe_id, flag):
        """
            Очистка ненужных компонент
        """

        sql_text = 'execute procedure RBS_Q_COMPONENT_AFTERIMPORT (?, ?)'
        sql_params = [recipe_id, flag]

        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams = sql_params,
                                            fetch='none',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_component_clear % recipe_id

            self._parent_class.log_file(message,
                                        terms=2,
                                        save_log_db=True)

    def _xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self._parent_class.xml_get_value_by_attr(xml, attr, flag)