# -*- coding: utf-8 -*-
# swat 26.06.2014
# version 0.0.2.0

"""
    класс импорта контрагентов
"""

import krconst as k

import plugins.impdata.object.impobject as imp_object
from rbsqutils import str_to_bool_int
from rbsqutils import check_number


class Customers(imp_object.Obj):
    """
        Класс импорта подразделений
    """

    def __init__(self, parent_class, flag_work, obj=None):
        """
            Инициализация переменных из XML
            если obj=None, то переменные нужно заполнять вручную
        """

        self.parent_class = parent_class
        self.child_class = type(self).__name__
        self.__obj = obj

        if obj is not None:
            self.code = self.xml_get_value(obj, 'code')
            self.name = self.xml_get_value(obj, 'name')
            self.parent = self.xml_get_value(obj, 'parent', flag='N')
            self.parentcode = self.xml_get_value(obj, 'parentcode', flag='N')
            self.parentgroup = self.xml_get_value(obj, 'parentgroup', flag='N')
            self.parentgroup = str_to_bool_int(self.parentgroup)
            self.isdelete = self.xml_get_value(obj, parent_class.xml_name_delete_flag, flag='N')
            self.isdelete = str_to_bool_int(self.isdelete)

            self.namefull = self.xml_get_value(obj, 'namefull', flag='N')
            self.address = self.xml_get_value(obj, 'address', flag='N')
            self.inn = self.xml_get_value(obj, 'inn', flag='N')
            self.inn = check_number(self.inn)
            self.edrpou = self.xml_get_value(obj, 'edrpou', flag='N')
            self.edrpou = check_number(self.edrpou)
            self.certificate_number = self.xml_get_value(obj, 'certificatenumber', flag='N')
            self.certificate_number = check_number(self.certificate_number)
            self.limit_use_by_date = self.xml_get_value(obj, 'limit_expiration_date', flag='N')

            if parent_class.xml_name_external_id:
                self.external_id = self.xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
                self.parent_id = self.xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')

            self.order_max_time = self.xml_get_value(obj, 'timeorder', flag='N')
            self.delivery_time = self.xml_get_value(obj, 'deliverytime', flag='N')
            if self.delivery_time:
                self.delivery_time = int(self.delivery_time)
            self.flag_work = flag_work
            self.type_object = 'C'

    def save_wsetproducer_customer(self):
        """
            Обновление параметра разбивка по производителю
        """

        makers_order = self.xml_get_value(self.__obj, 'makersorder', flag='N')
        if not makers_order:
            makers_order = '0'
        makers_order = str_to_bool_int(makers_order)

        ''' проверим есть ли параметр makersorder '''
        if makers_order and self.__obj.get('makersorder'):
            sql_text = 'execute procedure RBS_Q_OBJ_UPDATE_WSETBYPRODUCER(?,?)'
            sql_params = [self.objid, makers_order]

            res = self.parent_class.ExecuteSQL(sql_text,
                                               sqlparams = sql_params,
                                               fetch='None',
                                               ExtVer=True)
            if res['status'] == k.kr_sql_error:
                message = k.m_e_i_object % self.code + k.m_e_i_object_update_wsetbyproducer + k.kr_term_double_enter
                self.parent_class.LogFile(message)

    def save_bank_account(self):
        """
            Сохранение р/с
        """

        accounts = self.__obj.find('accounts')
        if accounts is not None:
            for account in accounts:
                code_bank = self.xml_get_value(account, 'codebank', flag='N')
                name = self.xml_get_value(account, 'name', flag='N')
                bank_account = self.xml_get_value(account, 'bankaccount', flag='N')

                if bank_account:
                    sql_text = 'execute procedure RBS_Q_BANKACC_INSEL(?,?,?,?)'
                    sql_params = [self.objid, code_bank, name, bank_account]

                    res = self.parent_class.ExecuteSQL(sql_text,
                                                       sqlparams = sql_params,
                                                       fetch='None',
                                                       ExtVer=True)
                    if res['status'] == k.kr_sql_error:
                        message = k.m_e_i_object_account % self.code + k.kr_term_double_enter
                        self.parent_class.LogFile(message)

    def save_print_data(self):
        """
            импорт данных для торг12
        """

        print_data = self.__obj.find('printdata')
        if print_data is not None:
            value = self.xml_get_value(print_data, 'value', flag='N')
            if value:
                sql_text = 'execute procedure RBS_Q_OBJ_UPDATE_PRINTDATA(?,?)'
                sql_params = [self.objid, value]

                res = self.parent_class.ExecuteSQL(sql_text,
                                                   sqlparams = sql_params,
                                                   fetch='none',
                                                   ExtVer=True)
                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_object_print_data % self.code + k.kr_term_double_enter
                    self.parent_class.LogFile(message)

    def save_activity(self):
        """
            импорт активности поставщиков
        """

        activities = self.__obj.find('cactivities')
        if activities is not None:
            for activity in activities:
                o_format = self.xml_get_value(activity, 'format', flag='N')
                no_activity_value = self.xml_get_value(activity, 'noactivityvalue', flag='N')
                no_activity_value = str_to_bool_int(no_activity_value)
                if no_activity_value == '0':
                        no_activity_value = '1'
                else:
                    if no_activity_value == '1':
                        no_activity_value = '0'

                sql_text = 'execute procedure RBS_Q_OBJ_UPDATE_STATUS(?,?,?)'
                sql_params = [self.objid, o_format, no_activity_value]
                res = self.parent_class.ExecuteSQL(sql_text,
                                                   sqlparams = sql_params,
                                                   fetch='none',
                                                   ExtVer=True)
                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_object_update_status % self.code + k.kr_term_double_enter
                    self.parent_class.LogFile(message)