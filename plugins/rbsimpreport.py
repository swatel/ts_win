# -*- coding: utf-8 -*-
"""
    swat 28.01.2014
    version 0.0.2.0
    модуль импорта отчетов импорта/экспорта
"""

import krconst as k
import BasePlugin as BP


class Plugin(BP.BasePlugin):
    """
        класс импорта отчетов
    """

    def run(self):
        """
            Импорт отчетов
        """

        xml_file = self.parse_file_xml(self.filenames)
        if self.result['result'] == k.plugin_error:
            return False

        report = xml_file.getroot()

        type_report = self.xml_get_value_by_attr(report, 'type')
        date = self.xml_get_value_by_attr(report, 'date')
        external_id = self.xml_get_value_by_attr(report, 'id1c', flag='N')
        id_rbs = self.xml_get_value_by_attr(report, 'idrbs', flag='N')
        status = self.xml_get_value_by_attr(report, 'status')
        massage = self.xml_get_value_by_attr(report, 'massage')
        sql_text = 'SELECT * FROM RBS_Q_REPORT_IMPORT(?, ?, ?, ?, ?, ?)'
        sql_params = [type_report, date, external_id,
                      id_rbs, status, massage]

        res = self.ExecuteSQL(sql_text,
                              sqlparams = sql_params,
                              fetch='one',
                              ExtVer=True)
        if res['status'] == k.sql_error:
            message = k.m_e_i_report
            self.log_file(message,
                          terms=2,
                          save_log_db=True)
        else:
            for itm in report:
                massage = self.xml_get_value_by_attr(itm, 'massage')
                sql_text = 'EXECUTE PROCEDURE RBS_Q_REPORT_IMPORT_WARNINGS(?, ?)'
                sql_params = [res['datalist']['reimportid'], massage]
                # необходима проверка, в случае ошибки писать в лог что то осмысленное
                ress = self.ExecuteSQL(sql_text,
                                       sqlparams = sql_params,
                                       fetch='none',
                                       ExtVer=True)
                if ress['status'] == k.sql_error:
                    message = k.m_e_i_report + '/n Не удалось выполнить запись в БД предупреждений по товарам! Файл ' + \
                              self.filenames
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)

        # получим codes
        '''code = self.xml_get_value_by_attr(report, 'code')

        if code == 'GETDOC':
            # запрос на экспорт документа
            docid = self.xml_get_value_by_attr(report, 'docid')
            idexternal = self.xml_get_value_by_attr(report, 'idexternal')
            docexppath = self.xml_get_value_by_attr(report, 'path')
            sql_text = ' EXECUTE PROCEDURE RBS_Q_IMP_IMPREPORTXML(?, ?, ?, ?)'
            sql_params = [docid, idexternal, code, docexppath]
            res = self.ExecuteSQL(sql_text,
                                  sqlparams = sql_params,
                                  fetch='none',
                                  ExtVer=True)
            if res['status'] == k.sql_error:
                self.LogFile(decodeXStr(res['message']))
                self.LogFile('Ид RBS:' + idexternal)
                self.LogFile('Ид внешней системы:' + idexternal)

            #idrbs = self.xml_get_value_by_attr(report, 'idrbs')
            #id1c = self.xml_get_value_by_attr(report, 'id1c')
            #sql = ' EXECUTE PROCEDURE RBS_Q_EXTERNALDOCUMENTUPDATE(?, ?)'
            #res = self.ExecuteSQL(sql,
            #                      sqlparams = [idrbs, id1c],
            #                      fetch='one',
            #                      ExtVer=True)
            #if res['status'] == krconst.kr_sql_error:
            #    self.LogFile(decodeXStr(res['message']))
            #    self.LogFile('Ид RBS:' + idrbs)
            #    self.LogFile('Ид 1c:' + id1c)
            '''