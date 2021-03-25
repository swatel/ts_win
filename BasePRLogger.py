# -*- coding: cp1251-*
"""
    24.03.2015
    ������ ����������� ������ �������-�����
"""

import time
import krconst as c
import os
import utils.a7z as arc


class RPLogger(object):
    """
        ����� �����������
    """

    def __init__(self, **kwargs):
        self.file_name = kwargs['path']
        self.cfg = kwargs['cfg']
        self.log_files_ize = kwargs['file_size']
        self.file = None

    def write(self, text, flag, type_message='INFO'):
        """
            ������ ����� � ���
        """

        current_time = time.strftime('%a %b %d %Y %H:%M:%S', time.localtime())
        start_error_log = current_time + ' ' + c.log_error + ': '

        try:
            if not os.access(self.file_name, os.F_OK):
                self.create_folder(self.file_name)
                self.file = open(self.file_name, "w")
            else:
                if os.path.getsize(self.file_name) / 1024 >= self.log_files_ize:
                    file_7z = str(self.file_name + current_time + '__' + '.7z')
                    file_7z = os.path.basename(file_7z)
                    file_7z = 'arc/' + file_7z.replace(':', '_')
                    if arc.pack_file(self.file_name, file_7z):
                        # todo ������� ������ � ������ �����
                        file_log = open(self.file_name, 'w')
                        file_log.close()

                self.file = open(self.file_name, "a")
            try:
                print>>self.file, current_time + ' ' + type_message + ': ' + text
                self.file.close()
            except:
                ''' error in text coding '''
                self.file = open(self.cfg.main_path + "/error.log", "a")
                if flag == c.kr_flag_logglobal:
                    self.cfg.global_log = 0
                    print>>self.file, start_error_log + c.m_e_global_log
                if flag == c.kr_flag_logplugin:
                    print>>self.file, start_error_log + c.m_e_plugin_log

                print>>self.file, start_error_log + c.m_e_log_text
                self.file.close()
                return False
        except:
            ''' file not found '''
            self.file = open(self.cfg.main_path + "/error.log", "a")
            if flag == c.kr_flag_logglobal:
                self.cfg.global_log = 0
                print>>self.file, start_error_log + c.m_e_global_log

            if flag == c.kr_flag_logplugin:
                self.file = open(self.cfg.main_path + "/error.log", "a")
                print>>self.file, start_error_log + c.m_e_plugin_log

            print>>self.file, start_error_log + c.m_e_file_not_found % self.file_name
            self.file.close()
            return False
        if flag == c.kr_flag_logplugin:
            return True

    #todo ��������� � �������
    def is_exists_folder(self, path):
        """
            �������� �� ������������� ��������
        """

        return os.path.exists(os.path.dirname(path) + '/')

    def create_folder(self, path):
        """
            �������� ��������
        """

        if not self.is_exists_folder(path):
            os.makedirs(os.path.dirname(path))
