# -*- coding: utf-8 -*-
# proper 31.03.2014
# version 0.0.2.0

"""
    класс импорта фирм
"""

import plugins.impdata.object.impobject as imp_object

from rbsqutils import str_to_bool_int, check_number


class Firms(imp_object.Obj):
    """
        Класс импорта фирм
    """

    def __init__(self, parent_class, flag_work, obj=None):
        """
            Инициализация переменных из XML
            если obj=None, то переменные нужно заполнять вручную
        """

        self.parent_class = parent_class
        self.child_class = type(self).__name__

        if obj is not None:
            self.code = self.xml_get_value(obj, 'code')
            self.name = self.xml_get_value(obj, 'name')
            self.parent = self.xml_get_value(obj, 'parent', flag='N')
            self.parentcode = self.xml_get_value(obj, 'parentcode', flag='N')
            self.realcode = self.xml_get_value(obj, 'realcode', flag='N')
            self.isdelete = self.xml_get_value(obj, parent_class.xml_name_delete_flag, flag='N')
            self.isdelete = str_to_bool_int(self.isdelete)
            self.parentgroup = self.xml_get_value(obj, 'parentgroup', flag='N')
            self.parentgroup = str_to_bool_int(self.parentgroup)

            self.namefull = self.xml_get_value(obj, 'namefull', flag='N')
            self.address = self.xml_get_value(obj, 'address', flag='N')
            self.inn = check_number(self.xml_get_value(obj, 'inn', flag='N'))
            self.edrpou = check_number(self.xml_get_value(obj, 'edrpou', flag='N'))
            self.certificatenumber = check_number(self.xml_get_value(obj, 'certificatenumber', flag='N'))

            if parent_class.xml_name_external_id:
                self.external_id = self.xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
                self.parent_id = self.xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')

        self.flag_work = flag_work
        self.type_object = 'F'





