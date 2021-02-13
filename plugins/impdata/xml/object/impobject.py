# -*- coding: utf-8 -*-
"""
    swat 09.01.2014
    version 0.0.2.0
    класс импорта объектов
"""

import krconst as k


class Obj():
    """
        Класс импорта объектов
    """

    parent_class = None
    child_class = None

    objid = None
    code = None
    name = None
    external_id = None
    parentcode = None
    parent = None
    parentgroup = None
    parent_id = None
    flag_work = None
    type_object = None
    isdelete = None
    realcode = None
    namefull = None
    address = None
    inn = None
    edrpou = None
    certificate_number = None
    highercode = None
    highername = None
    highertype = None
    mfo = None
    format = None

    result_class = k.plugin_ok

    def __init__(self):
        pass

    def xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self.parent_class.xml_get_value_by_attr(xml, attr, flag)

    def save(self):
        """
            Сохранение
        """

        if self.parentcode == '0':
            self.parentcode = None

        sql_text = 'select * from RBS_Q_OBJ_INSSEL (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [self.code, self.name, self.namefull,
                      self.flag_work, self.type_object,
                      self.parentcode, self.address, self.isdelete,
                      self.inn, self.edrpou,
                      self.certificate_number, self.parentgroup,
                      self.highercode, self.highername,
                      self.highertype, self.mfo, self.realcode,
                      self.parent, self.external_id, self.parent_id]

        res = self.parent_class.ExecuteSQL(sql_text,
                                           sqlparams = sql_params,
                                           fetch='one',
                                           ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_object % self.code
            self.parent_class.log_file(message,
                                       terms=0,
                                       save_log_db=True)
            message = k.kr_message_error_importtypeobject % self.child_class
            self.parent_class.log_file(message,
                                       terms=2,
                                       save_log_db=True)
            self.result_class = k.plugin_error
        else:
            self.objid = res['datalist']['OBJID']

            ''' Если формат заполнен то сохраним его '''

            if self.format:
                pass
                sql_text = 'execute procedure RBS_Q_FORMATOBJ_INSSEL(?,?)'
                sql_params = [self.format, self.objid]
                res = self.parent_class.ExecuteSQL(sql_text,
                                                   sqlparams = sql_params,
                                                   fetch='none',
                                                   ExtVer=True)
                if res['status'] == k.kr_sql_error:
                    self.parent_class.LogFile(k.m_e_i_format_obj % (self.format, self.code) + k.kr_term_double_enter)