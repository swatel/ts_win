# -*- coding: utf-8 -*-
"""
    swat 28.01.2014
    version 0.0.1.9
    модуль проверки импорта и отправки почты о результатах
"""

import krconst as c
import BasePlugin as BP
import rbssendemail as e
from datetime import datetime as d


class Plugin(BP.BasePlugin):
    """
        проверки импорта и отправки почты о результатах
    """

    def run(self):
        """
            проверка
        """

        body_report = self.get_import_report_body('0')
        ''' параметры будем получать из БД '''
        if body_report:
            a = e.Email(self,
                        smtp_server=self.ParserXML(self.taskparamsxml, 'smtp_server'),
                        port=self.ParserXML(self.taskparamsxml, 'port'),
                        username=self.ParserXML(self.taskparamsxml, 'username'),
                        password=self.ParserXML(self.taskparamsxml, 'password'),
                        from_address=self.ParserXML(self.taskparamsxml, 'from_address'),
                        use_tls=self.ParserXML(self.taskparamsxml, 'use_tls'),
                        to_address=self.ParserXML(self.taskparamsxml, 'to_address'),
                        subject=self.ParserXML(self.taskparamsxml, 'subject'),
                        message=body_report)
            if a.send_email():
                ''' обновление статуса таблицы с отчетами в БД '''
                sql_text = 'EXECUTE PROCEDURE RBS_Q_REPORT_IMPORT_CHANGEST'
                sql_params = []
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='none',
                                      ExtVer=True)
                if res['status'] == c.sql_error:
                    message = c.m_e_check_import
                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)

    def get_import_report_body(self, status):
        """
            формируем тело письма отчета об ошибках импорта
        """

        body_report = None
        sql_text = 'SELECT * FROM RBS_Q_REPORT_IMPORT_GET(?, ?)'
        sql_params = ['0', status]
        res = self.ExecuteSQL(sql_text,
                              sqlparams = sql_params,
                              fetch='many',
                              ExtVer=True)
        if res['status'] == c.sql_error:
            message = c.m_e_check_import
            self.log_file(message,
                          terms=2,
                          save_log_db=True)
        else:
            if len(res['datalist']) > 0:
                per_id = 0
                body_report = 'Отчет об ошибках обмена RBS(' + d.now().strftime('%d.%m.%Y %H:%M:%S') + ') : \n'
                for itm in res['datalist']:
                    if itm['IMPORTID'] != per_id:
                        body_report = body_report + '\nДокумент № ' + str(itm['DOCID']) + '  ' + str(itm['DESCRIPT']) + '\n'
                        body_report = body_report + ' ' + str(itm['DESCRIPT_W']) + '\n'
                        per_id = itm['IMPORTID']
                    else:
                        body_report = body_report + ' ' + str(itm['DESCRIPT_W']) + '\n'
        return body_report