# -*- coding: utf-8 -*-
"""
    Модуль начальной конфигурации сервер-задач
"""

import os
import sys
import xml.etree.ElementTree as Et
# import elementtree.ElementTree as Et

import krconst as c


class KConfig(object):
    """
        Класс начальной конфигурации сервер-задач
    """

    def __init__(self, name_db):
        self.main_path = os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')
        self.name_db = name_db
        self.file_name_config = self.main_path + "/config.xml"
        self.xml_cfg = None
        self.db_ip = None
        self.db_path = None
        self.db_role = None
        self.db_charset = None
        self.db_user = None
        self.db_pass = None
        self.db_list = {}

        ''' Параметры перерыва '''
        self.break_params = {}

        ''' Статус сервера-задач '''
        self.status_config = c.kr_status_config_ok
        self.status_config_message = ''
        self.status_server_queue = None

        ''' Флаг потери коннекта '''
        self.LostConnectDB = None

        ''' Глобальные параметры '''
        self.global_log = None
        self.global_sleep_interval = None
        self.global_def_dir_tmp_files = None
        self.global_dir_report = None
        self.global_tmpl_report = None
        self.global_tmpl_report_alcodecl = None
        self.global_dir_tmp_clear = None
        self.global_def_dir_tmp_clear_interval = None
        self.engine_dir_files = None

        ''' Параметры ОС '''
        self.os_version = None
        self.os_platform = None

        ''' параметры для работы со слоями '''
        self.layers_work = False
        self.layers_engine_db = None
        self.layers_code = None
        self.layers_id = None

    def get_config_file(self):
        """
            Получение файла конфигурации
        """

        if not os.access(self.file_name_config, os.F_OK):
            self.status_config = c.kr_status_config_error
            self.status_config_message = c.m_e_file_not_found % self.file_name_config
            self.xml_cfg = None
        else:
            try:
                self.xml_cfg = Et.parse(self.file_name_config)
            except:
                self.xml_cfg = None
                self.status_config = c.kr_status_config_error
                self.status_config_message = c.m_e_xml_parse_error % self.file_name_config

    def get_config_layer(self):
        """
            Получение параметров работы со слоями
        """

        if self.xml_cfg:
            self.layers_work = self.xml_cfg.find("layers_work").attrib["value"]

    def get_db_by_attr(self, root, attrib, value):
        """
            Получение атрибута
        """

        return root.find(".//db[@" + attrib + "='" + value + "']")

    def get_config(self, layer_code='', layer_conf=None):
        """
            Получение параметров работы сервера-задач
        """

        if self.xml_cfg:
            self.global_def_dir_tmp_files = self.xml_cfg.find("globaldefdirtmpfiles").attrib["value"]
            if layer_conf:
                try:
                    # os.sep в конце обязателен - для некоторых плагинов критично (например checkdir)
                    self.global_def_dir_tmp_files = os.path.join(self.global_def_dir_tmp_files, layer_code) + os.sep
                    self.db_ip = layer_conf.db_ip
                    dbs_storage = self.xml_cfg.find("dbs_storage").attrib["value"]
                    self.db_path = os.path.join(dbs_storage, layer_code + '-MYSHOP.FDB')
                    self.db_role = layer_conf.db_role
                    self.db_charset = layer_conf.db_charset
                except:
                    self.status_config = c.kr_status_config_error
                try:
                    self.db_user = layer_conf.db_user
                    self.db_pass = layer_conf.db_pass
                except:
                    pass
            else:
                root = self.xml_cfg.getroot()
                db_config = self.get_db_by_attr(root, "name", str(self.name_db))
                if db_config is None:
                    self.status_config_message = c.m_e_xml_bad_key % (self.file_name_config, str(self.name_db))
                try:
                    self.db_ip = db_config.get("ip")
                    self.db_path = db_config.get("path")
                    self.db_role = db_config.get("db_role")
                    self.db_charset = db_config.get("db_charset")
                except:
                    self.status_config = c.kr_status_config_error
                try:
                    self.db_user = db_config.get("db_user")
                    self.db_pass = db_config.get("db_pass")
                except:
                    pass
            self.global_log = int(self.xml_cfg.find("globallog").attrib["value"])
            self.global_sleep_interval = float(self.xml_cfg.find("globalsleepinterval").attrib["value"])
            self.global_dir_report = self.xml_cfg.find("globaldirreport").attrib["value"]
            self.global_tmpl_report = self.xml_cfg.find("globaltmplreport").attrib["value"]
            self.global_tmpl_report_alcodecl = self.xml_cfg.find("global_tmpl_report_alcodecl").attrib["value"]
            self.global_dir_tmp_clear = self.xml_cfg.find("globaldirtmpclear") is not None and \
                                        self.xml_cfg.find("globaldirtmpclear").attrib["value"] == '1'
            self.global_def_dir_tmp_clear_interval = 7
            try:
                if self.xml_cfg.find("globa_defdirtmpclearinterval") is not None:
                    self.global_def_dir_tmp_clear_interval = int(self.xml_cfg.find("globa_defdirtmpclearinterval").attrib["value"])
            except ValueError:
                pass
            if self.xml_cfg.find("engine_dir_files") is not None:
                self.engine_dir_files = self.xml_cfg.find("engine_dir_files").attrib["value"]

    def get_os_version(self):
        """
            Получение параметров ОС на которой работает сервер-задач
        """

        import platform
        if 'win' in sys.platform:
            self.os_platform = 'win'
            self.os_version = platform.win32_ver()[0]
        if 'linux' in sys.platform:
            self.os_platform = 'linux'
            self.os_version = platform.dist()[0]
            if self.os_version == '':
                self.os_version = 'linux'
