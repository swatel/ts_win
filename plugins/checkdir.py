# -*- coding: utf-8 -*-
"""
    swat 03.12.2014
    модуль опроса каталога
"""

import os
import glob

import krconst
import BasePlugin as Bp

version = '0.0.2.2'


class Plugin(Bp.BasePlugin):
    """
        Класс проверки каталога для импорта
    """

    check_dir = None
    check_dir2 = None
    mask_files = None
    sub_folders = None
    copy_too = None
    copy_too_dir = None
    copy_too_mask = None
    file_list = []

    def run(self):
        """
            Запуск плагина на исполнение
        """

        if self.taskparamsxml:
            # подключение сетевого ресурса при необходимости
            self.mount_dir()

            self.check_dir = self.parser_xml(self.taskparamsxml, 'CheckDir').replace('\\', '/')
            try:
                self.check_dir2 = self.parser_xml(self.taskparamsxml, 'CheckDir2').replace('\\', '/')
            except:
                pass

            self.mask_files = self.parser_xml(self.taskparamsxml, 'MaskFiles')

            self.log_file('Check folder ' + self.check_dir, terms=1)

            # проверим существует ли данный каталог если нет, то предупреждение в лог файл
            if not self.is_exists_folder(self.check_dir):
                self.log_file(krconst.m_e_not_exists_folder % self.check_dir)

            self.sub_folders = self.ParserXML(self.taskparamsxml, 'SubFolders')
            if self.layer_code != '':
                # если работа со слоями то всегда будем сканировать подкаталоги
                self.sub_folders = '1'

            if self.result['result'] == krconst.plugin_error:
                return False

            # проверим есть ли флаг дополнительного копирования файла
            try:
                self.copy_too = self.ParserXML(self.taskparamsxml, 'CopyToo')
                self.copy_too_dir = self.ParserXML(self.taskparamsxml, 'CopyTooDir')
                self.copy_too_mask = self.ParserXML(self.taskparamsxml, 'CopyTooMask')
            except:
                self.copy_too = '0'

            self.file_list = []
            self.check_dir_by_path(self.check_dir)

            if self.file_list:
                for itm in self.file_list:
                    file_queueid = self.check_file_in_queue_sort(itm, 'CheckDir')
                    if file_queueid:
                        ''' Скопируем дополнително файл '''
                        if self.copy_too == '1':
                            self.check_file_mask(itm)
                        copy_dist = self.parent.k_conf.global_def_dir_tmp_files
                        copy_dist = copy_dist + 'Q' + str(file_queueid) + '/' + os.path.basename(itm)
                        if self.copy_file(itm, copy_dist):
                            self.log_file(krconst.m_i_file_create_task_ok % itm.replace('\\', '/'))
                            s_params = None
                            if self.sub_folders == '1':
                                s_path = os.path.dirname(itm)
                                s_path = s_path.replace('\\', '/')
                                s_path = s_path.replace(self.check_dir, '')
                                if s_path != '':
                                    s_params = '''<params>
    <sub_folder value="%s"/>
</params>''' % s_path
                            self.update_status_turn_db(file_queueid, krconst.kr_status_new, params=s_params)
                            if not self.delete_tmp_file(itm):
                                self.log_file(krconst.m_e_delete_file % itm.replace('\\', '/'))
                                self.result['result'] = krconst.plugin_error
                                return False

            if self.check_dir2:
                self.check_dir_by_path(self.check_dir2)

                if self.file_list:
                    for itm in self.file_list:
                        file_queueid = self.check_file_in_queue_sort(itm, 'CheckDir')
                        if file_queueid:
                            ''' Скопируем дополнително файл '''
                            if self.copy_too == '1':
                                self.check_file_mask(itm)
                            copy_dist = self.parent.k_conf.global_def_dir_tmp_files
                            copy_dist = copy_dist + 'Q' + str(file_queueid) + '/' + os.path.basename(itm)
                            if self.copy_file(itm, copy_dist):
                                self.log_file(krconst.m_i_file_create_task_ok % itm.replace('\\', '/'))
                                s_params = None
                                if self.sub_folders == '1':
                                    s_path = os.path.dirname(itm)
                                    s_path = s_path.replace('\\', '/')
                                    s_path = s_path.replace(self.check_dir2, '')
                                    if s_path != '':
                                        s_params = '''<params>
        <sub_folder value="%s"/>
    </params>''' % s_path
                                self.update_status_turn_db(file_queueid, krconst.kr_status_new, params=s_params)
                                if not self.delete_tmp_file(itm):
                                    self.log_file(krconst.m_e_delete_file % itm.replace('\\', '/'))
                                    self.result['result'] = krconst.plugin_error
                                    return False
        else:
            self.result['result'] = krconst.plugin_error
            self.log_file(krconst.m_e_params_is_null)

    def check_dir_by_path(self, path):
        """
            Проверка каталога на наличие файла по маске файла
        """

        if not self.mask_files:
            self.mask_files = '*'
        for mask in self.mask_files.split(','):
            file_list = sorted(glob.glob(path + '/' + mask), key=os.path.getmtime)
            for itm in file_list:
                if os.path.isfile(itm):
                    self.file_list.append(itm)
                elif self.sub_folders == '1':
                    if os.path.isdir(itm):
                        self.check_dir_by_path(itm)

    def check_file_mask(self, filename):
        """
            Проверка имени файла по маске и копирование
        """

        sql_text = 'select * from RBS_Q_CHECKFILEMASK (?, ?) R'
        sql_params = [filename, self.copy_too_mask]

        res = self.ExecuteSQL(sql_text,
                              sqlparams=sql_params,
                              fetch='one',
                              ExtVer=True)

        if res['status'] == krconst.kr_sql_error:
            self.log_file('Ошибка проверки имени файла по маске', terms=2)
        else:
            if res['datalist']['ISGOODFILE'] == '1':
                if not self.copy_file(filename, self.copy_too_dir + '/' + os.path.basename(filename)):
                    self.log_file('Ошибка копирования файла в дополнительную директорию', terms=2)
