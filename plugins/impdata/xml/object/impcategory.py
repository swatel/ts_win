# -*- coding: utf-8 -*-
"""
    swat 10.01.2014
    version 0.0.2.0
    модуль импорта групп
"""

import krconst

from rbsqutils import str_to_bool_int


class Category():
    """
        Класс импорта групп объектов
    """

    parent_class = None

    catid = None
    code = None
    name = None
    external_id = None
    parent_code = None
    parent = None
    parent_id = None
    flag_work = None
    type_object = None
    is_delete = None

    def __init__(self, parent_class, flag_work, type_object, obj=None):
        """
            Инициализация переменных из XML
        """

        self.parent_class = parent_class

        if obj is not None:
            self.code = self.xml_get_value(obj, 'code')
            self.name = self.xml_get_value(obj, 'name')
            self.is_delete = self.xml_get_value(obj, 'deletemarker')
            self.is_delete = str_to_bool_int(self.is_delete)
            self.parent = self.xml_get_value(obj, 'parent')
            self.parent_code = self.xml_get_value(obj, 'parentcode', flag='N')

            if parent_class.xml_name_external_id:
                self.external_id = self.xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
                self.parent_id = self.xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')
        self.flag_work = flag_work
        self.type_object = type_object

    def save(self):
        """
            Сохранение
        """

        if self.parent_code == '0':
            self.parent_code = None

        sql_params = [self.name, self.code, self.parent_code,
                      self.flag_work, self.type_object,
                      self.is_delete, self.external_id, self.parent_id]
        sql_text = 'select * from RBS_Q_CATEGORY_INSSEL(?,?,?,?,?,?,?,?)'

        res = self.parent_class.ExecuteSQL(sql_text,
                                           sqlparams=sql_params,
                                           fetch='one',
                                           ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            message = krconst.m_e_importcategory % self.code
            self.parent_class.log_file(message,
                                       terms=2,
                                       save_log_db=True)
        else:
            self.catid = res['datalist']['CATID']

    def xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self.parent_class.xml_get_value_by_attr(xml, attr, flag)