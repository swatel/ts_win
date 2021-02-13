# -*- coding: utf-8 -*-
# swat 27.06.2014
# version 0.0.2.0

"""
    класс импорта производителей
"""

import plugins.impdata.object.impobject as imp_object


class Makers(imp_object.Obj):
    """
        Класс импорта производителей
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
            self.namefull = self.xml_get_value(obj, 'namefull', flag='N')

            if parent_class.xml_name_external_id:
                self.external_id = self.xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
                self.parent_id = self.xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')

        self.flag_work = flag_work
        self.type_object = 'P'