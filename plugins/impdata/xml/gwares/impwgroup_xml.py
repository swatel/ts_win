# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    модуль импорта групп товаров из XML
"""

from rbsqutils import str_to_bool_int

import plugins.impdata.impwgroup_base as w


class WGroupXML(w.BaseWGroup):
    """
        Класс  импорта групп товаров из XML
    """

    def __init__(self, parent_class, obj):
        """
            Инициализация переменных из XML
        """

        self.parent_class = parent_class

        self.code = self.__xml_get_value(obj, 'warescode', flag='N')
        self.name = self.__xml_get_value(obj, 'waresname', flag='N')
        self.delete_marker = self.__xml_get_value(obj, parent_class.xml_name_delete_flag, flag='N')
        self.delete_marker = str_to_bool_int(self.delete_marker)
        self.parent = self.__xml_get_value(obj, 'parent', flag='N')
        self.parent_code = self.__xml_get_value(obj, 'parentcode', flag='N')

        if parent_class.xml_name_external_id:
            self.external_id = self.__xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
            self.group_id = self.__xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')
            if not self.group_id:
                self.group_id = self.__xml_get_value(obj, 'parent' + parent_class.xml_name_external_id, flag='N')

        if self.parent_code == '0':
            self.parent_code = None

        ''' для поддержки старых форматов '''
        if not self.code:
            self.code = self.__xml_get_value(obj, 'code', flag='N')
        if not self.name:
            self.name = self.__xml_get_value(obj, 'name', flag='N')

        self.singularity = self.__xml_get_value(obj, 'singularity', flag='N')
        self.egaisneed = self.__xml_get_value(obj, 'egaisneed', flag='N')

    def __xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self.parent_class.xml_get_value_by_attr(xml, attr, flag)
