# -*- coding: utf-8 -*-

import glob
import os

import clientbank as kb
import BasePlugin as Bp

#test
"""params = {'sa_url': 'https://sa-test.litebox.ru',
          'sa_username': '_system_admin_test',
          'sa_password': '12A345QWEzS',
          'mask_files': '*.txt',
          # Фильтр на тип документов
          'doc_types': ['Платежное поручение'],
          # Фильтр на получателя
          'recipients': ['6316202773'],  # 6316202773 - ООО "Облачный ритеил"
          'regex_email': "^.*?([0-9a-zA-Z][a-zA-Z0-9._-]*@[a-zA-Z0-9._-]+\.[a-zA-Z]{2,6}).*$",
          'regex_uid': "^.*?л/с[\s]*([0-9]{1,9}).*$",
          'regex_num': "^.*?([0-9]-[0-9]{8}).*$",
          'sort_dir': '../../../examples/bank/sort',
          'out_dir': '../../../examples/bank/out',
          'error_dir': '../../../examples/bank/error',
          'domain_in_dir': 'in'
          }
          """
# release
params = {'sa_url': 'http://in.litebox.ru',
          'sa_username': '_system_admin_pro',
          'sa_password': '98b705ac3',
          'mask_files': '*.txt',
          # Фильтр на тип документов
          'doc_types': ['Платежное поручение'],
          # Фильтр на плательщика
          'ignore_inn': ['7841016636'],
          # Фильтр на получателя
          'recipients': ['6316202773'],  # 6316202773 - ООО "Облачный ритеил"
          'regex_email': "([0-9a-zA-Z][a-zA-Z0-9._-]*@[a-zA-Z0-9._-]+\.[a-zA-Z]{2,6})",
          'regex_uid': "[лЛ][\\\\/]?[сС][чЧ]*\s*[№]*\s*([0-9]{1,9})",
          'regex_num': "^.*?([0-9]-[0-9]{8}).*$",
          'sort_dir': '../../../examples/bank/sort',
          'out_dir': '../../../examples/bank/out',
          'error_dir': '../../../examples/bank/error',
          'domain_in_dir': 'in'
          }


class Plugin(Bp.BasePlugin):
    """
        Класс разбора платежек из 1С
    """

    def run(self):
        """
        Запуск
        """

        # проверим есть ли файлы в папке
        # если нет, то не запускаем дальше обработку
        path = self.parser_xml(self.taskparamsxml, 'path')

        dir_in = os.path.join(path, 'Входящие')
        dir_in = dir_in.decode('cp1251')

        dir_out = os.path.join(path, 'Исходящие')
        dir_out = dir_out.decode('cp1251')

        dir_sort = os.path.join(path, 'Сортировка')
        dir_sort = dir_sort.decode('cp1251')

        dir_error = os.path.join(path, 'Ошибки')
        dir_error = dir_error.decode('cp1251')

        self.log_file('', terms=1)
        self.log_file('Проверям ' + os.path.join(path, 'Входящие'), terms=1)
        file_list_in = glob.glob(dir_in + '/*.*')
        self.log_file('Файлов для обработки в папке Входящие = ' + str(len(file_list_in)), terms=1)
        self.log_file('Проверям ' + os.path.join(path, 'Сортировка'), terms=1)
        file_list_sort = glob.glob(dir_sort + '/*.*')
        self.log_file('Файлов для обработки в папке Сортировка = ' + str(len(file_list_sort)), terms=1)

        if len(file_list_in) > 0 or len(file_list_sort) > 0:
            # заменим правильно параметры работы плагина
            params['sort_dir'] = os.path.join(path, dir_sort)
            params['out_dir'] = os.path.join(path, dir_out)
            params['error_dir'] = os.path.join(path, dir_error)
            params['domain_in_dir'] = 'Входящие'
            b_c = kb.BankClient(params)
            b_c.sort(self, dir_in)

if __name__ == "__main__":
    bc = kb.BankClient(params)
    bc.sort(None, '../../../examples/bank/in')
