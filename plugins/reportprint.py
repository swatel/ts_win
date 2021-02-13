# -*- coding: utf-8 -*-
"""
    swat 21.01.2014
    version 0.0.2.0
    модуль печати
"""

import os
import time
import subprocess
import configparser

#from StringIO import StringIO

import krconst
import BasePlugin as Bp


class Plugin(Bp.BasePlugin):
    """
        Класс печати
    """

    section_procedure_name = 'ProcNames'
    section_procedure_param = 'ProcParams'
    section_procedure_fetch = 'ProcFetches'
    section_datalist = 'ProcDataList'
    section_dimension = 'Dimension'
    section_convert = 'Convert'
    report_config = None
    kwards_print = None
    file_name_report_print = None
    type_convert = 'pdf'

    def run(self):
        """
            Запуск
        """

        if self.queueparamsxml:
            self.report_config = None
            self.kwards_print = None
            
            """ определить где находится файл с настройками """
            sql_text = 'select R.FILENAME from RBS_Q_GETFILEREPORT' + '(?) R'
            res = self.ExecuteSQL(sql_text,
                                  sqlparams = [self.rule])
            if res[0] == krconst.kr_sql_error:
                self.LogFile(res[1])
            else:
                if res[2]:
                    report_file_name = res[2][0]['FILENAME']
                else:
                    self.log_file(krconst.kr_message_error_errorreportimdb % self.rule)
                    self.result['result'] = krconst.plugin_error
                    return False  

            report_params = self.XMLGetAllParams(self.queueparamsxml, asdic=True)
            if self.result['result'] == krconst.plugin_error:
                return False
            
            if self.printer(report_file_name, **report_params):
                pass
                """ удаляем файл """
                #if not self.delete_tmp_file(self.file_name_report_print):
                #    self.LogFile(krconst.kr_message_error_errordeletefile % self.file_name_report_print)
                #    self.result['result'] = krconst.plugin_error
                #    return False
            return True
        
        else:
            self.result['result'] = krconst.plugin_error
            self.log_file(krconst.m_e_params_is_null)
            return False
        
    def create_print_report_file(self, html, file_name):
        """
            Преобразование html -> pdf
            Преобразование html -> jpg
        """

        tmp_file_html = (os.path.basename(file_name)).split('.')[0] + '.html'
        tmp_file_html = os.path.join(os.path.dirname(file_name), tmp_file_html)
        tmp_file = open(tmp_file_html, "w")
        print(html, file=tmp_file)
        tmp_file.close()

        dimension = self.get_dimension_from_file()

        convert_str = ''
        dimension_str = ''

        if self.type_convert == 'pdf':
            if dimension:
                dimension_str = ' --page-width ' + dimension['width'] + \
                                'mm --page-height ' + dimension['height'] + 'mm '
            convert_str = 'wkhtmltopdf -L 0 -T 0 -R 0 -B 0 ' + dimension_str + tmp_file_html + ' ' + file_name + ' >> ./log/stdout.log 2>&1'

        if self.type_convert == 'jpg':
            dimension_str = '--width ' + dimension['width'] + ' --height ' + dimension['height'] + ' '
            convert_str = 'wkhtmltoimage -f jpg ' + dimension_str + tmp_file_html + ' ' + file_name + ' >> ./log/stdout.log 2>&1'

        ''' преобразуем html в нужный формат '''
        #todo вывод в лог сделать параметром
        str_echo = 'echo $SHELL' + ' >> ./log/stdout.log 2>&1'
        os.system(str_echo)
        str_echo = 'echo $PATH' + ' >> ./log/stdout.log 2>&1'
        os.system(str_echo)
        self.log_file(convert_str + krconst.kr_term_double_enter)
        pdf = os.system(convert_str)

        if pdf != 0:
            self.log_file('Ошибка конвертации html')
            self.result['result'] = krconst.plugin_error

        ''' удаляем файл '''
        #if not self.delete_tmp_file(tmp_file_html):
        #    self.log_file(krconst.kr_message_error_errordeletefile % tmp_file_html)
        #    self.result['result'] = krconst.plugin_error
        #    return False
    
    def print_file_report(self, file_name_report):
        """
            проверим нужно ли печатать данный документ на принтер
            если PrintAfterDone = '1' или парметра нет, то печатаем на принтер
            иначе не печатаем
        """

        # todo вынести печать в отдельный модуль

        print_after_done = '1'
        try:
            print_after_done = self.ParserXML(self.queueparamsxml, 'PrintAfterDone')
        except:
            self.log_file(krconst.kr_message_error_printnoparams % 'PrintAfterDone')
        if print_after_done == '0':
            return True
        
        """ проверим есть ли в параметрах принтер на который печатать """
        name_printer = self.ParserXML(self.queueparamsxml, 'printer')
        
        if not name_printer:
            self.log_file(krconst.kr_message_error_printnoparams % 'printer')
            self.result['result'] = krconst.plugin_error
            return False
        
        """ печать на windows """
        if self.parent.k_conf.os_platform == 'win':
            try:
                import win32api
            except:
                self.log_file(krconst.kr_message_error_errorrimportlib % 'win32api')
                self.result['result'] = krconst.plugin_error
                return False
            """ если ОС XP то печать на идет на любой принтер и нормально возвращает сообщние об ошибке
            # на других системах печатает, но непонятен результат """
            try:
                win32api.ShellExecute(0, "printto", '"' + os.path.join(os.getcwd(), file_name_report) + '"',
                                      '"%s"' % name_printer, ".", 0)
                return True
            except:
                self.TracebackLog(krconst.kr_message_error_errorreportpdfprint % file_name_report)
                self.result['result'] = krconst.plugin_error
                return False
        
        """ печать на linux """
        if self.parent.k_conf.os_platform == 'linux':
            self.log_file('lp -d ' + name_printer + ' ' + file_name_report + krconst.kr_term_double_enter)
            str_print = 'lp -d ' + name_printer + ' ' + file_name_report
            return self.print_file_report_linux(str_print)
            #try:
            #    self.log_file('lp -d ' + name_printer + ' ' + file_name_report + krconst.kr_term_double_enter)
            #    os.system('lp -d ' + name_printer + ' ' + file_name_report)
            #except:
            #    self.LogFile(krconst.kr_message_error_errorreportpdfprint % file_name_report)
            #    self.log_file('lp  -o page-bottom=0 -o page-top=0 -o page-left=0 -o page-right=0 -o media=a4 -o scaling=100 -d ' + name_printer + ' ' + file_name_report + krconst.kr_term_double_enter)
            #    os.system('lp  -o page-bottom=0 -o page-top=0 -o page-left=0 -o page-right=0 -o media=a4 -o scaling=100 -d ' + name_printer + ' ' + file_name_report)
            #    self.result['result'] = krconst.plugin_error
            #    return False
        return True

    def print_file_report_linux(self, str_print, repetition=True):
        """
            Печать под Linux
        """

        result = False

        pipe = subprocess.PIPE
        process = subprocess.Popen(str_print,
                                   shell=True,
                                   stdin=pipe,
                                   stdout=pipe,
                                   stderr=subprocess.STDOUT)
        process.poll()
        str_process = process.stdout.readlines()[0]
        if process.returncode:
            self.result['result'] = krconst.plugin_error
            try:
                self.log_file(str_process)
            except:
                self.log_file('Ошибка обработки результата при наличии ошибки в печати.')
        else:
            # проверим что в логе
            try:
                self.log_file(str_process)
                if 'request id is' in str_process:
                    result = True
                else:
                    if 'lp: successful-ok' in str_process:
                        if repetition:
                            # служба не работает, ждем 2 сек и перезапуск
                            time.sleep(2)
                            result = self.print_file_report_linux(str_print, repetition=False)
                        else:
                            self.result['result'] = krconst.plugin_error
                    else:
                        self.result['result'] = krconst.plugin_error
                        self.log_file('Неизвестная ошибка.')
                    return result
            except:
                self.result['result'] = krconst.plugin_error
                self.log_file('Ошибка обработки результата.')
        return result
    
    def printer(self, report_file_name, **kwargs):

        self.kwards_print = kwargs

        procedures = self.get_proc_from_file(os.path.join(self.parent.k_conf.global_dir_report, report_file_name))
        out_data = {}
        for item in procedures:
            tmp = self.ExecuteSQL(item['procname'], 
                                  sqlparams=item['procparams'],
                                  fetch=item['fetch'],
                                  ExtVer=True)
            if tmp['status'] == krconst.kr_sql_error:
                return False
            if item['fetch'] == 'all':
                out_data[item['dl']] = tmp['datalist']
            else:
                out_data[item['dl']] = tmp['datalist']
        
        out_data['html'] = '\n\t' + self.report_config.get('html', 'html').replace('<', '\n\t<').replace('#', '\n\t#')
        out_data['html'] = out_data['html'].decode('utf8').encode('cp1251')
        
        tmpl_report = self.parent.k_conf.global_tmpl_report
        report_full_file_name = self.parent.k_conf.global_dir_report + '.' + tmpl_report
        exec('from %s import %s' % (report_full_file_name, tmpl_report))
        html_report = str(locals()[tmpl_report](searchList = [out_data]))

        ''' получим тип конвертации файла, для печати '''
        self.get_type_convert()
        file_name_report = os.path.join(self.parent.k_conf.global_def_dir_tmp_files,
                                        'reportQ' + str(self.queueid) + '.' + self.type_convert)
        self.create_print_report_file(html_report, file_name_report)

        if self.result['result'] != krconst.plugin_error:
            if self.print_file_report(file_name_report):
                return False
            else:
                self.file_name_report_print = file_name_report
                return True
        
        #todo изменить правильный флаг http://pythonworld.ru/moduli/modul-os.html
        '''if os.access(file_name_report, os.F_OK):
            if self.print_file_report(file_name_report):
                return False
            else:
                self.file_name_report_print = file_name_report
                return True
        else:
            self.LogFile(krconst.kr_message_error_errorreportpdf % file_name_report)
            self.result['result'] = krconst.plugin_error
            return False'''
    
    def get_proc_from_file(self, file_name, get_name = None):
        self.report_config = configparser.ConfigParser()
        self.report_config.read(file_name)
        procedures = []
        for item in self.report_config.options(self.section_procedure_name):
            procedures.append({
                'procname': self.report_config.get(self.section_procedure_name, item),
                'procparams': self.get_params_to_proc(item, get_name),
                'fetch': self.get_fetch_from_proc(item),
                'dl': self.get_dl_from_proc(item)
            })
        return procedures
    
    def get_params_to_proc(self, name_param, get_name = None):
        params = self.report_config.get(self.section_procedure_param, name_param)
        params_mas = []
        if get_name is not None:
            return params
        for item in params.split(','):
            val = self.kwards_print[item]
            if val == 'None':
                params_mas.append(None)
            else:
                params_mas.append(val)
        return params_mas
        
    def get_fetch_from_proc(self, name_param):
        if self.report_config.has_section(self.section_procedure_fetch):
            if self.report_config.has_option(self.section_procedure_fetch, name_param):
                return self.report_config.get(self.section_procedure_fetch, name_param)
        return 'all'
    
    def get_dl_from_proc(self, name_param):
        if self.report_config.has_section(self.section_datalist):
            if self.report_config.has_option(self.section_datalist, name_param):
                return self.report_config.get(self.section_datalist, name_param)

    def get_dimension_from_file(self):
        """
            Получить размерностей этикетки
        """

        result = {}
        if self.report_config.has_section(self.section_dimension):
            result['width'] = self.report_config.get(self.section_dimension, 'width')
            result['height'] = self.report_config.get(self.section_dimension, 'height')
        return result

    def get_type_convert(self):
        """
           Получим тип конвертации html файла
           по умолчанию pdf
        """

        convert = None
        if self.report_config.has_section(self.section_convert):
            convert = self.report_config.get(self.section_convert, 'name')
        if convert:
            self.type_convert = convert