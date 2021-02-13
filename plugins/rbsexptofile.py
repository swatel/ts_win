# -*- coding: utf-8 -*-
"""
    swat 05.03.2012
    version 0.0.2.2
    модуль экспорта данных в файл
"""

import os
import time
import datetime
import json

import krconst as c
import BasePlugin as Bp

from rbsqutils import decodeXStr
from rbsqutils import pack_file


#todo все сообщения перевести на krconst
class Plugin(Bp.BasePlugin):
    """
        Класс экспорта в файл
    """

    odb = None
    export_dic = None
    master_id = None
    detail_params = None
    version = None

    def run(self):
        """
            Запуск плагина
        """

        # получим параметры экспорта
        self.export_dic = self.xml_get_all_params(self.ruleparams, as_dic=True)
        if self.result['result'] == c.plugin_error:
            return False

        self.version = self.get_params_rule('version')

        header_select = self.get_params_rule('HeaderSelect')
        master_header_select = self.get_params_rule('MasterHeaderSelect')
        if self.version:
            detail_count = int(self.get_params_rule('detail_count'))
            self.detail_params = self.get_params_rule('detail_params')
            i = 1
            detail_select = {}
            while i <= detail_count:
                detail_select[str(i)] = self.get_params_rule('DetailSelect' + str(i))
                i += 1
        else:
            # версия по умолчанию
            detail_select = self.get_params_rule('DetailSelect')
        master_footer_select = self.get_params_rule('MasterFooterSelect')
        footer_select = self.get_params_rule('FooterSelect')

        # дополнительные параметры
        doc_bond_header_select = self.get_params_rule('DocbondHeaderSelect')
        doc_bond_select = self.get_params_rule('DocbondSelect')
        doc_bond_footer_select = self.get_params_rule('DocbondFooterSelect')

        # попробуем получить кодировку экспорта
        file_encoding = self.get_params_rule('FileEncoding')

        # определим есть ли экспорта дополнительные параметры
        advanced_params = self.get_params_rule('AdvancedParams', False)

        # нужно ли паковать результат
        pack_file_name = self.get_params_rule('PackFileName')

        # Формат файла экспорта
        result_format = self.get_params_rule('result_format', 'xml')

        # кол-во макроподстановок
        result_marco_cnt = int(self.get_params_rule('marco_cnt', 2))

        # обработка правила
        rule_dic = self.xml_get_all_params(self.queueparamsxml, as_dic=True)
        if self.result['result'] == c.plugin_error:
            return False

        # получим параметр для ХП, первый обязательный это id
        sql_params = []
        sql_params.append(rule_dic['IDs'])
        # получим дополнительные параметры
        if advanced_params:
            for item in self.get_params_rule('NameSPParams').split(','):
                if item == 'queueid':
                        sql_params.append(self.queueid)
                else:
                    try:
                        if rule_dic[item] in ('Null', 'None', 'NULL', 'NONE'):
                            sql_params.append(None)
                        else:
                            sql_params.append(rule_dic[item])
                    except:
                        sql_params.append(None)
        # получим код БД, если нужен экспорт из другой БД
        db_code = None
        self.odb = None
        try:
            db_code = rule_dic['DBCODE']
        except:
            pass
        if db_code:
            odb_cfg = self.read_config_other_db(db_code)
            if not odb_cfg:
                self.log_file('Нет настроек доп БД!')
                self.result['result'] = c.plugin_error
                return False
            else:
                self.odb = self.connect_other_db(odb_cfg)
                if not self.odb:
                    self.log_file('Нет подключения к доп БД!')
                    self.result['result'] = c.plugin_error
                    return False

        # получим имя файла если оно указано
        file_name_exp = rule_dic['FileName']
        if not file_name_exp:
            self.log_file('Неизвестен файл назначения')
            self.result['result'] = c.plugin_error
            return False

        # начинаем выполнение процедур для экспорта
        header_select_str = self.execute_params_sql(header_select, sql_params)
        if self.version:
            res = self.execute_params_sql(master_header_select, sql_params, 'datalist')
            master_header_select_str = ''
            for itm in res:
                sql_params_detail = []
                self.master_id = itm[self.detail_params]
                sql_params_detail.append(self.master_id)
                master_header_select_str += self.sql_res_to_str(itm)
                i = 1
                detail_select_str = ''
                while i <= detail_count:
                    detail_select_str_temp = self.execute_params_sql(detail_select[str(i)], sql_params_detail)
                    if len(detail_select_str_temp) > 1 and detail_select_str_temp:
                        detail_select_str += detail_select_str_temp
                    i += 1
                master_header_select_str += detail_select_str
                master_footer_select_str = self.execute_params_sql(master_footer_select, sql_params)
                master_header_select_str += master_footer_select_str
            detail_select_str = ''
            master_footer_select_str = ''
        else:
            master_header_select_str = self.execute_params_sql(master_header_select, sql_params)
            detail_select_str = self.execute_params_sql(detail_select, sql_params)
            master_footer_select_str = self.execute_params_sql(master_footer_select, sql_params)


        try:
            result_main = header_select_str + master_header_select_str + detail_select_str + master_footer_select_str
        except:
            self.log_file('Один из результирующих запросов возвращает NULL')
            self.log_to_db('Один из результирующих запросов возвращает NULL')
            self.result['result'] = c.plugin_error
            return False

        # проверим нужно ли возвращать docbond
        result_doc_bond = ''
        if doc_bond_header_select:
            docbond_header_select_str = self.execute_params_sql(doc_bond_header_select, sql_params)
            doc_bond_select_str = self.execute_params_sql(doc_bond_select, sql_params)
            doc_bond_footer_select_str = self.execute_params_sql(doc_bond_footer_select, sql_params)
            if doc_bond_select_str:
                try:
                    if len(docbond_header_select_str) > 1:
                        result_doc_bond += docbond_header_select_str
                    if len(doc_bond_select_str) > 1:
                        result_doc_bond += doc_bond_select_str
                    if len(doc_bond_footer_select_str) > 1:
                        result_doc_bond += doc_bond_footer_select_str
                    result_doc_bond = docbond_header_select_str + doc_bond_select_str + doc_bond_footer_select_str
                except:
                    self.log_file('Один из результирующих запросов возвращает NULL')
                    self.log_to_db('Один из результирующих запросов возвращает NULL')
                    self.result['result'] = c.plugin_error
                    return False

        # проверим есть ли FooterSelect
        footer_select_str = ''
        if footer_select:
            footer_select_str = self.execute_params_sql(footer_select, sql_params)

        result = result_main + result_doc_bond + footer_select_str

        # формирование имени файла
        now = datetime.datetime.now()
        s1 = 'temp_doc_'
        s2 = time.strftime('%Y%m%d%H%M%S', time.localtime()) + str(now.microsecond)

        if result_marco_cnt == 2:
            try:
                file_name_exp_temp = (file_name_exp % (s1, s2)).replace('\\', '/')
                file_name_exp = (file_name_exp % ('', s2)).replace('\\', '/')
            except:
                file_name_exp_temp = (file_name_exp % s2).replace('\\', '/')
                file_name_exp_temp = file_name_exp_temp.replace('doc', s1)
                file_name_exp = (file_name_exp % s2).replace('\\', '/')
        if result_marco_cnt == 1:
            file_name_exp_temp = (file_name_exp % s1).replace('\\', '/')
            file_name_exp = (file_name_exp % '').replace('\\', '/')

        # проверим результирующий формат
        if result_format == 'json':
            file_name_exp_temp = file_name_exp_temp.replace('.xml', '.json')
            file_name_exp = file_name_exp.replace('.xml', '.json')

        ''' проверим существует ли каталог для экспорта файла
            проверям на существование переменной, для поддержания старых заданий
        '''
        try:
            exp_path = rule_dic['ExpPath'].replace('\\', '/')
        except:
            exp_path = None
        if exp_path:
            if not self.is_exists_folder(exp_path):
                ''' подключение сетевого ресурса при необходимости '''
                self.mount_dir()
                if not self.is_exists_folder(exp_path):
                    self.log_file(c.m_e_not_exists_folder % exp_path)
                    self.log_to_db(c.m_e_not_exists_folder % exp_path)
                    self.result['result'] = c.plugin_error
                    return False

        ''' преобразование кодировки файла '''
        try:
            if result_format == 'xml':
                if file_encoding:
                    result = result.decode('cp1251').encode(file_encoding)
            #if result_format == 'json':
            #    result = json.dumps(result, encoding='cp1251')
            #    result = json.dumps(result, encoding='cp1251')
        except:
            self.TracebackLog('Ошибка перекодирования файла')
            self.log_to_db('Ошибка перекодирования файла')

        ''' сохраняем сначала во временный файл '''
        try:
            self.text_save_to_file(result, file_name_exp_temp)
        except:
            self.TracebackLog('Ошибка сохранения во временый файл')
            self.log_to_db('Ошибка сохранения во временый файл')

        ''' проверим есть ли флаг дополнительного копирования файла '''
        copy_too = '0'
        if self.taskparamsxml:
            try:
                copy_too = self.ParserXML(self.taskparamsxml, 'CopyToo')
                copy_too_dir = self.ParserXML(self.taskparamsxml, 'CopyTooDir')
            except:
                copy_too = '0'

        if copy_too == '1':
            copy_too_dir = os.path.join(copy_too_dir, os.path.basename(file_name_exp))
            ''' переименовываем временный файл в нормальный в доп структуру '''
            try:
                self.copy_file(file_name_exp_temp, copy_too_dir)
            except:
                self.TracebackLog('Ошибка переименования файла в доп структуру')
                self.log_to_db('Ошибка переименования файла в доп структуру')

        ''' переименовываем временный файл в нормальный '''
        try:
            self.move_file(file_name_exp_temp, file_name_exp)
            self.export_file_name = file_name_exp
        except:
            self.TracebackLog('Ошибка переименования файла')
            self.log_to_db('Ошибка переименования файла')

        ''' упаковка в случае необходимости '''
        if pack_file_name:
            if not pack_file(file_name_exp, pack_file_name):
                self.result['result'] = c.plugin_error
                self.log_file('Ошибка архивирования файла.')
                self.log_to_db('Ошибка архивирования файла.')

        if self.result['result'] == c.kr_sql_error:
            self.db.rollback()
        else:
            self.db.commit()

    def execute_params_sql(self, sql_text, params, type_result='str'):
        """
            Выполнение запросов
        """

        if '(?' not in sql_text:
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
            result = ''
            # если версия не по умолчанию то мы возвращаем не строку а запись datalist
            if type_result == 'str':
                for itm in res_sql['datalist']:
                    result += self.sql_res_to_str(itm)
            else:
                result = res_sql['datalist']

        return result

    def sql_res_to_str(self, row):
        """
            Преобразование sql в тест
        """
        str_result = ''
        for key in list(row.keys()):
            if key != self.detail_params:
                str_result = str_result + row[key] + c.kr_term_enter
        return str_result

    def get_params_rule(self, key, default=None):
        """
            Получение параметров правила экспорта
        """

        try:
            result = self.export_dic[key]
        except KeyError:
            result = default
        return result
