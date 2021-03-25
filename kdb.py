# -*- coding: cp1251-*
"""
    swat 20.11.2014
    version 0.0.2.2
    Базовый модуль для работы с различными БД (не FB)
"""

import os.path

import krconst as c
import kconfig as kc


class BaseDBConfig(kc.KConfig):
    """
        Базовый класс получения конфигурации конфигурации к различными БД
    """

    def __init__(self, name_db):
        self.main_path = os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')
        self.file_name_config = self.main_path + "/config.xml"
        self.name_db = name_db
        self.connectstring = None
        self.xml_cfg = None
        self.status_config = c.kr_status_config_ok
        self.status_config_message = ''

        self.get_config_file()
        self.get_config()

    def get_config(self):
        """
            Получение строки конфигурации
        """
        if self.xml_cfg:
            root = self.xml_cfg.getroot()
            db = self.get_db_by_attr(root, "name", str(self.name_db))
            if db is None:
                self.status_config = c.kr_status_config_error
                self.status_config_message = c.m_e_xml_bad_key % (self.file_name_config,
                                                                  str(self.name_db))
            try:
                self.connectstring = db.get("connectstring")
            except:
                self.status_config = c.kr_status_config_error
                self.status_config_message = c.kr_message_error_connectstring
        else:
            self.status_config = c.kr_status_config_error

#todo добавить класс, для работы с базой
