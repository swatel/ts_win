# -*- coding: utf-8 -*-
"""
    swat 26.08.2014
    version 0.0.2.0
    класс импорта из временных структур imp_*
    базовый модуль
"""

import BasePlugin

import krconst as k

import plugins.impdata.object.impdolgn as imp_dolgn
import plugins.impdata.object.impdepartmens as imp_dep
import plugins.impdata.object.impman as imp_man

import plugins.impdata.gwares.impunit as imp_unit
import plugins.impdata.impwgroup_base as imp_wgroup


class Plugin(BasePlugin.BasePlugin):
    """
        Класс импорта данных из временных структур
    """

    ''' ид сессии импорта '''
    imp_session = None
    sql_text_id_get = 'select * from IMP_ID_GET (?,?)'
    sql_text_id_update = 'execute procedure imp_id_update(?,?,?)'

    def run(self):
        """
            Импорт данных
        """

        dic_params = self.XMLGetAllParams(self.queueparamsxml, asdic=True)

        '''  получим ид сессии импорта '''
        self.imp_session = dic_params['imp_session_id']

        ''' импорт проходит по логическим блокам данных '''

        ''' должности '''
        self.import_dolgn()

        ''' подразделения '''
        self.import_depart()

        ''' сотрудники EMPLOYEE'''
        self.import_employee()

        ''' ед измерения '''
        self.import_unit()

        ''' группы товаров '''
        self.import_wares_group()

        ''' товары '''
        self.import_gwares()

        ''' ед измерения товара '''
        self.import_wares_unit()

        self.end_import()

    def get_id_data(self, content):
        """
            получение id сущности для импорта
        """

        result = []
        sql_params = [self.imp_session, content]
        res = self.ExecuteSQL(self.sql_text_id_get,
                              sqlparams = sql_params,
                              fetch='many',
                              ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_get_tmp_id_get % content
            self.log_file(message,
                          terms=2,
                          save_log_db=True)
        else:
            for itm in res['datalist']:
                result.append({'DATA_ID': itm['DATA_ID']})
        return result

    def update_status(self, content, data_id, status):
        """
            Обновление статуса записи
        """

        result = True
        sql_params = [data_id, content, status]
        res = self.ExecuteSQL(self.sql_text_id_update,
                              sqlparams = sql_params,
                              fetch='none',
                              ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_get_tmp_id_update % content
            self.log_file(message,
                          terms=2,
                          save_log_db=True)
            result = False

        return result

    def end_import(self):
        """
            Завершение импорта
        """

        if self.result['result'] == k.plugin_ok:
            status = '1'
        else:
            status = 'E'

        sql_text = 'execute procedure IMP_END(?, ?)'
        sql_params = [self.imp_session, status]

        res = self.ExecuteSQL(sql_text,
                              sqlparams = sql_params,
                              fetch='none',
                              ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_get_tmp_end % self.imp_session
            self.log_file(message,
                          terms=2,
                          save_log_db=True)

    def import_dolgn(self):
        """
            импорт должностей
        """

        dic = self.get_id_data('DOLGN')
        sql_text = 'select * from IMP_DOLGN_GET(?)'
        for itm in dic:
            data_id = itm['DATA_ID']

            ''' переведем запись в статус i '''
            if self.update_status('DOLGN', data_id, 'i'):
                sql_params = [data_id]

                ''' получим данные '''
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)

                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_get_tmp_id_dolgn % data_id
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)
                else:
                    dolgn = imp_dolgn.Dolgn(self)
                    dolgn.dolgnguid = res['datalist']['GUID']
                    dolgn.dolgncode = res['datalist']['CODE']
                    dolgn.dolgnname = res['datalist']['NAME']
                    dolgn.save()

                    ''' если сохрание прошло успешно, ставим статус 1 '''
                    if dolgn.dolgnid:
                        self.update_status('DOLGN', data_id, '1')

    def import_depart(self):
        """
            импорт подразделений
        """

        dic = self.get_id_data('DEPART')
        sql_text = 'select * from IMP_DEPART_GET(?)'

        for itm in dic:
            data_id = itm['DATA_ID']

            ''' переведем запись в статус i '''
            if self.update_status('DEPART', data_id, 'i'):
                sql_params = [data_id]

                ''' получим данные '''
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)

                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_get_tmp_id_depart % data_id
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)
                else:
                    dep = imp_dep.Departmens(self, 'I')
                    dep.code = res['datalist']['CODE']
                    dep.name = res['datalist']['NAME']
                    dep.parent = res['datalist']['HIGHERNAME']
                    dep.parentcode = res['datalist']['HIGHERCODE']
                    dep.isdelete = res['datalist']['ISDELETE']
                    dep.parentgroup = '0'
                    dep.external_id = res['datalist']['GUID']
                    dep.parent_id = res['datalist']['HIGHERGUID']
                    dep.save()

                    ''' если сохрание прошло успешно, ставим статус 1 '''
                    if dep.objid:
                        self.update_status('DEPART', data_id, '1')

    def import_employee(self):
        """
            импорт сотрудников
        """

        dic = self.get_id_data('EMPLOYEE')
        sql_text = 'select * from IMP_EMPLOYEE_GET(?)'
        for itm in dic:
            data_id = itm['DATA_ID']

            ''' переведем запись в статус i '''
            if self.update_status('EMPLOYEE', data_id, 'i'):
                sql_params = [data_id]

                ''' получим данные '''
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)

                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_get_tmp_id_employee % data_id
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)
                else:
                    man = imp_man.Man(self, 'I')
                    man.code = res['datalist']['CODE']
                    man.name = res['datalist']['FULLNAME']
                    man.isdelete = res['datalist']['ISDELETE']
                    man.parentgroup = '0'
                    man.external_id = res['datalist']['GUID']
                    man.save()

                    dolgn = imp_dolgn.Dolgn(self)
                    dolgn.dolgnguid = res['datalist']['DOLGNGUID']
                    dolgn.dolgncode = res['datalist']['DOLGNCODE']
                    dolgn.dolgnname = res['datalist']['DOLGNNAME']
                    dolgn.save()

                    man.dolgnid = dolgn.dolgnid
                    man.set_dolgn_man()

                    ''' если сохрание прошло успешно, ставим статус 1 '''
                    if man.objid:
                        self.update_status('EMPLOYEE', data_id, '1')

    def import_unit(self):
        """
            импорт ед измерения
        """

        dic = self.get_id_data('UNITS')
        sql_text = 'select * from IMP_UNITS_GET(?)'
        for itm in dic:
            data_id = itm['DATA_ID']

            ''' переведем запись в статус i '''
            if self.update_status('UNITS', data_id, 'i'):
                sql_params = [data_id]

                ''' получим данные '''
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)

                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_get_tmp_id_unit % data_id
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)
                else:
                    unit = imp_unit.Unit(self)
                    unit._external_id = res['datalist']['GUID']
                    unit._external_code = res['datalist']['SHORTNAME']
                    unit._short_name = res['datalist']['SHORTNAME']
                    unit. _full_name = res['datalist']['NAME']
                    unit.save()

                    ''' если сохрание прошло успешно, ставим статус 1 '''
                    if unit.unit_id:
                        self.update_status('UNITS', data_id, '1')

    def import_wares_group(self):
        """
            импорт групп товаров
        """

        dic = self.get_id_data('WARESGROUP')
        sql_text = 'select * from IMP_WARESGROUP_GET(?)'
        for itm in dic:
            data_id = itm['DATA_ID']

            ''' переведем запись в статус i '''
            if self.update_status('WARESGROUP', data_id, 'i'):
                sql_params = [data_id]

                ''' получим данные '''
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)

                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_get_tmp_id_wgroup % data_id
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)
                else:
                    wgroup = imp_wgroup.BaseWGroup()
                    wgroup.parent_class = self
                    wgroup.code = res['datalist']['CODE']
                    wgroup.name = res['datalist']['NAME']
                    wgroup.delete_marker = res['datalist']['ISDELETE']
                    wgroup.parent = res['datalist']['HIGHERNAME']
                    wgroup.parent_code = res['datalist']['HIGHERCODE']
                    wgroup.external_id = res['datalist']['GUID']
                    wgroup.group_id = res['datalist']['HIGHERGUID']
                    wgroup.save()

                    ''' если сохрание прошло успешно, ставим статус 1 '''
                    if wgroup.wgroup_id:
                        self.update_status('WARESGROUP', data_id, '1')

    def import_gwares(self):
        """
            импорт товаров
        """

        dic = self.get_id_data('GWARES')
        sql_text = 'select * from IMP_GWARES_GET(?)'
        sql_text_ins = 'select * from  RBS_Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        for itm in dic:
            data_id = itm['DATA_ID']

            ''' переведем запись в статус i '''
            if self.update_status('GWARES', data_id, 'i'):
                sql_params = [data_id]

                ''' получим данные '''
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)

                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_get_tmp_id_wares % data_id
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)
                else:
                    wares_id = None
                    _code = res['datalist']['CODE']
                    _name = res['datalist']['NAME']
                    _delete_marker = res['datalist']['ISDELETE']
                    _parent = res['datalist']['WGROUPNAME']
                    _parent_code = res['datalist']['WGROUPCODE']
                    _external_id = res['datalist']['GUID']
                    _group_id = res['datalist']['WARESGROUOPGUID']

                    sql_params_ins = [_name, _code, None, _parent_code, None,
                                      None, _delete_marker, 'I', None,
                                      None, _parent, None,
                                      None, _external_id, None, _group_id,
                                      None]

                    res = self.ExecuteSQL(sql_text_ins,
                                          sqlparams = sql_params_ins,
                                          fetch='one',
                                          ExtVer=True)
                    if res['status'] == k.kr_sql_error:
                        self.log_file(k.m_e_i_wares % _code + k.kr_term_double_enter)
                    else:
                        wares_id = res['datalist']['WARESID']

                    ''' если сохрание прошло успешно, ставим статус 1 '''
                    if wares_id:
                        self.update_status('GWARES', data_id, '1')

    def import_wares_unit(self):
        """
            импорт ед измерения товаров
        """

        dic = self.get_id_data('WARESUNIT')
        sql_text = 'select * from IMP_WARESUNIT_GET(?)'
        sql_text_ins = 'select * from RBS_Q_WARESUNIT_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_text_upd = 'update waresunit wu set wu.externalcode =?, wu.externalid =? where wu.waresunitid =?'
        for itm in dic:
            data_id = itm['DATA_ID']

            ''' переведем запись в статус i '''
            if self.update_status('WARESUNIT', data_id, 'i'):
                sql_params = [data_id]

                ''' получим данные '''
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)

                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_get_tmp_id_wareunit % data_id
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)
                else:
                    wares_unit_id = None
                    _code = res['datalist']['WARESCODE']
                    _external_id = res['datalist']['WARESGUID']
                    _factor = res['datalist']['FACTOR']
                    _unit = res['datalist']['UNITGUID']
                    _wu_code = res['datalist']['CODE']
                    _wu_guid = res['datalist']['GUID']

                    if _code:
                        sql_params_ins = [_code, _unit, str(_factor), 'mainunit',
                                          None, None, None, None, None, 1,
                                          None, _external_id]

                        res = self.ExecuteSQL(sql_text_ins,
                                              sqlparams = sql_params_ins,
                                              fetch='one',
                                              ExtVer=True)
                        if res['status'] == k.kr_sql_error:
                            self.log_file(k.m_e_i_wares_unit % _code + k.kr_term_double_enter)
                        else:
                            wares_unit_id = res['datalist']['WARESUNITID']

                            ''' обновим external поля '''
                            sql_params_upd = [_wu_code, _wu_guid, wares_unit_id]
                            res_upd = self.ExecuteSQL(sql_text_upd,
                                                      sqlparams = sql_params_upd,
                                                      fetch='none',
                                                      ExtVer=True)

                        ''' если сохрание прошло успешно, ставим статус 1 '''
                        if wares_unit_id:
                            self.update_status('WARESUNIT', data_id, '1')