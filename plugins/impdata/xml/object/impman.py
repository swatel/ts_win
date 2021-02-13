# -*- coding: utf-8 -*-
# swat 09.01.2014
# version 0.0.2.0

"""
    класс импорта физ лиц + при необходимости добавлять пользователей
"""

import plugins.impdata.object.impobject as imp_object
import plugins.impdata.object.impdolgn as imp_dolgn

import krconst as k
from rbsqutils import str_to_bool_int


class Man(imp_object.Obj):
    """
        Класс импорта физ лиц
    """

    dolgnid = None

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
            self.parent = self.xml_get_value(obj, 'parent')
            self.parentcode = self.xml_get_value(obj, 'parentcode', flag='N')
            self.realcode = self.xml_get_value(obj, 'realcode', flag='N')
            self.isdelete = self.xml_get_value(obj, 'deletemarker')
            self.isdelete = str_to_bool_int(self.isdelete)
            self.parentgroup = self.xml_get_value(obj, 'parentgroup')
            self.parentgroup = str_to_bool_int(self.parentgroup)

            if parent_class.xml_name_external_id:
                self.external_id = self.xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
                self.parent_id = self.xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')

        self.flag_work = flag_work
        self.type_object = 'M'

    def check_ignore_user(self):
        """
            проверка на игнорируемые имена
        """
        result = True
        if self.parent_class.user_ignore_name:
            if ',' + self.name + ',' in self.parent_class.user_ignore_name:
                message = k.m_w_ignore_user % self.name
                self.parent_class.log_file(message,
                                           terms=2,
                                           save_log_db=True)
                result = False
        return result

    def set_dolgn(self, obj):
        """
            присвоение должности
        """

        dolgn = imp_dolgn.Dolgn(self.parent_class, obj)
        dolgn.save()
        self.dolgnid = dolgn.dolgnid

        if dolgn.result_class == k.plugin_error:
            self.result_class = k.plugin_error

    def set_dolgn_man(self):
        """
            установка должности сотруднику
        """
        if self.dolgnid and self.objid:
            sql_text = 'execute procedure RBS_Q_USER_SETDOLGN(?, ?, ?)'
            sql_params = [self.objid, self.dolgnid, self.isdelete]

            res = self.parent_class.ExecuteSQL(sql_text,
                                               sqlparams = sql_params,
                                               fetch='none',
                                               ExtVer=True)
            if res['status'] == k.kr_sql_error:
                message = k.m_e_i_set_dolgn % self.code
                self.parent_class.log_file(message,
                                           terms=0,
                                           save_log_db=True)
                self.result_class = k.plugin_error