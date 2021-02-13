# -*- coding: utf-8 -*-

"""
    Экспорт данных в xls
"""

import os

import krconst as c
import BasePlugin as Bp
from rbsqutils import decodeXStr

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '21.07.2015'


class Plugin(Bp.BasePlugin):
    """
        Класс экспорта данных в xls
    """

    odb = None

    def run(self):
        """
            Запуск плагина
        """

        ''' обработка правила '''
        rule_dic = self.xml_get_all_params(self.queueparamsxml, as_dic=True)
        if self.result['result'] == c.plugin_error:
            return False

        sql_text = rule_dic['sql_text']
        file_name = rule_dic['file_name']
        fields = rule_dic['fields']

        ''' начинаем выполнение процедур для экспорта '''
        datalist = self.execute_params_sql(sql_text, [])
        if datalist:
            try:
                f = open(file_name, 'wb')
            except:
                self.log_file('Ошибка доступа к ресурсу экспорта')
                self.log_to_db('Ошибка доступа к ресурсу экспорта')
                self.result['result'] = c.plugin_error
                return False
            linerec = ''
            for key in fields.split(','):
                linerec += key + '\t'
            linerec += '\r\n'
            f.writelines(linerec)
            for itm in datalist:
                linerec = ''
                for key in fields.split(','):
                    linerec += self.val_to_str(itm[key]) + '\t'
                linerec += '\r\n'
                f.writelines(linerec)
            f.close()

        ''' проверим есть ли флаг дополнительного копирования файла '''
        try:
            copy_too = rule_dic['CopyToo']
            copy_too_dir = rule_dic['CopyTooDir']
        except:
            copy_too = '0'
        if copy_too == '1':
            copy_too_dir = os.path.join(copy_too_dir, os.path.basename(file_name))
            ''' переименовываем временный файл в нормальный в доп структуру '''
            try:
                self.copy_file(file_name, copy_too_dir)
            except:
                self.TracebackLog('Ошибка переименования файла в доп структуру')
                self.log_to_db('Ошибка переименования файла в доп структуру')

    def execute_params_sql(self, sql_text, params):
        """
            Выполнение запросов
        """
        if not '(?' in sql_text:
            params = []
        res_sql = self.ExecuteSQL(sql_text,
                                  sqlparams=params,
                                  fetch='many',
                                  db_local=self.odb,
                                  ExtVer=True,
                                  auto_commit=False)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file(decodeXStr(res_sql['message']))
            self.log_file(c.kr_term_enter + c.kr_term_enter)
            return None
        else:
            result = res_sql['datalist']
        return result

    def sql_res_to_str(self, row):
        """
            Преобразование sql в тест
        """
        str_result = ''
        for key in list(row.keys()):
            str_result = str_result + row[key] + c.kr_term_enter
        return str_result

    @staticmethod
    def val_to_str(param=None):
        if param is not None:
            return str(param)
        else:
            return str('')
