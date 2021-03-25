# -*- coding: windows-1251 -*-
﻿# coding=utf-8
"""
    Печать html
"""

__author__ = 'swat'

import os
import subprocess
import string
import random
import sys
import traceback

VERSION = '0.0.0.2'


class PrintHtml(object):

    __url_or_text = None
    __printer_name = None
    __type_html = 'text'
    __type_convert = None
    __page_width = None
    __page_height = None
    __convert_file_name_pdf = None
    __result_convert = False
    __os_platform = ''
    __tmp_dir = None

    __margin_bottom = 10
    __margin_top = 10
    __margin_left = 10
    __margin_right = 10

    result = False
    message = None

    def __init__(self, url_or_text, printer_name, tmp_dir, type_html='text',
                 type_convert='pdf', page_width=None, page_height=None,
                 margin_bottom = 10, margin_top = 10, margin_left = 10, margin_right = 10):

        self.__url_or_text = url_or_text
        self.__printer_name = printer_name
        self.__type_html = type_html
        self.__type_convert = type_convert
        self.__tmp_dir = tmp_dir
        self.__page_width = page_width
        self.__page_height = page_height

        self.__margin_bottom = margin_bottom
        self.__margin_top = margin_top
        self.__margin_left = margin_left
        self.__margin_right = margin_right

    def convert(self):
        """
            Конвертация html в необходимый для печати формат
        """

        if self.__type_convert not in('pdf', 'jpg'):
            self.result = False
            self.message = 'Неверный формат преобразования.'
        else:
            if self.__type_html not in('text', 'url'):
                self.result = False
                self.message = 'Неверный тип html.'
            else:
                if not self.__tmp_dir:
                    self.result = False
                    self.message = 'Не передана временная папка.'
                else:
                    convert_file_name = os.path.join(self.__tmp_dir,
                                                     self.__rand_string(10))
                    self.__convert_file_name_pdf = convert_file_name + '.pdf'

                    if self.__type_html == 'text':
                        tmp_file_html = convert_file_name + '.html'
                        tmp_file = open(tmp_file_html, "w")
                        print>>tmp_file, self.__url_or_text.encode('utf-8')
                        tmp_file.close()
                        self.__url_or_text = tmp_file_html

                    str_convert = ''
                    str_dimension = ''
                    if self.__page_width and self.__page_height:
                        str_dimension = '--page-width ' + self.__page_height + 'mm ' + \
                                        '--page-height ' + self.__page_width + 'mm '
                    if self.__type_convert == 'pdf':
                        str_convert += 'wkhtmltopdf -L ' + str(self.__margin_left) +\
                                       ' -T ' + str(self.__margin_top) +\
                                       ' -R ' + str(self.__margin_right) +\
                                       ' -B ' + str(self.__margin_bottom)

                    if self.__type_convert == 'jpg':
                        str_convert += 'wkhtmltoimage -f jpg '

                    str_convert += str_dimension + ' ' + self.__url_or_text + ' ' + self.__convert_file_name_pdf
                    pipe = subprocess.PIPE
                    process = subprocess.Popen(str_convert,
                                               shell=True,
                                               stdin=pipe,
                                               stdout=pipe,
                                               stderr=subprocess.STDOUT)
                    process.poll()
                    if process.returncode:
                        self.__result_convert = False
                        self.result = False
                        self.message = process.stdout.readlines()
                    else:
                        self.__result_convert = True
                        self.result = True
                        #if self.__type_html == 'text':
                        #    self.delete_file(self.__url_or_text)
        return self.result

    def print_page(self):
        """
            Печать
        """
        if self.__result_convert:
            self.get_os_version()

            # печать на windows
            if self.__os_platform == 'win':
                try:
                    import win32api
                except:
                    return False
                # если ОС XP то печать на идет на любой принтер и нормально возвращает сообщние об ошибке
                # на других системах печатает, но непонятен результат
                try:
                    win32api.ShellExecute(0, "printto", self.__convert_file_name_pdf, '"%s"' % self.__printer_name, ".", 0)
                    return True
                except:
                    self.__traceback_log()
                    return False

            # печать на linux
            if self.__os_platform == 'linux':
                try:
                    os.system('lp -d ' + self.__printer_name + ' ' + self.__convert_file_name_pdf)
                except:
                    self.result = False
        else:
            self.result = False
            self.message = 'Была ошибка конвертации файла!'
        return self.result

    @staticmethod
    def __rand_string(n):
        """
            Произвольная строка
        """
        a = string.ascii_letters + string.digits
        return ''.join([random.choice(a) for i in range(n)])

    def get_os_version(self):
        """
            Получение параметров ОС на которой работает сервер-задач
        """

        if 'win' in sys.platform:
            self.__os_platform = 'win'
        if 'linux' in sys.platform:
            self.__os_platform = 'linux'

    def __traceback_log(self):
        """
            Обработка исключений
        """

        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_ = str(exc_value)
        tb = traceback.extract_tb(exc_traceback)
        for err in tb:
            tb_ += '\n'
            for er in err:
                tb_ += str(er) + ', '
        message = self.__decode_str(tb_)
        return message

    @staticmethod
    def __decode_str(text):
        """
            Перекодирует строку
        """

        text = str(text)
        letter_list = text.split('\\x')
        ret = ''
        first = True
        for letter_code in letter_list:
            if not first:
                try:
                    code = int(letter_code[:2], 16)
                    ret += chr(code) + letter_code[2:]
                except:
                    ret += '\\x%s' % letter_code
            else:
                first = False
                ret += '%s' % letter_code
        return ret

    @staticmethod
    def delete_file(file_name):
        """
            Удаление файла
        """
        try:
            os.unlink(file_name)
        except:
            raise
            return False