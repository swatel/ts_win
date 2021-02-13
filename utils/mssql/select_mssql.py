# -*- coding: utf-8 -*-
# coding=utf-8

import pyodbc
import re
import os
import traceback
import sys
#import elementtree.ElementTree as Et
import xml.etree.ElementTree as Et
import os.path

__author__ = 'swat'
VERSION = '0.0.0.1'


class BaseDBConfig(object):
    """
        Базовый класс получения конфигурации конфигурации к различными БД
    """

    def __init__(self, name_db):
        self.main_path = os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')
        self.file_name_config = self.main_path + "/config.xml"
        self.name_db = name_db
        self.connectstring = None
        self.xml_cfg = None
        self.status_config = 1
        self.status_config_message = ''

        self.get_config_file()
        self.get_config()

    def get_config_file(self):
        """
            Получение файла конфигурации
        """

        if not os.access(self.file_name_config, os.F_OK):
            self.status_config = 0
            self.status_config_message = 'Файл %s не найден' % self.file_name_config
            self.xml_cfg = None
        else:
            try:
                self.xml_cfg = Et.parse(self.file_name_config)
            except:
                self.xml_cfg = None
                self.status_config = 0
                self.status_config_message = 'Ошибка обработки xml файла: %s . XML файл не валидный.' \
                                             % self.file_name_config

    def get_config(self):
        """
            Получение строки конфигурации
        """
        if self.xml_cfg:
            root = self.xml_cfg.getroot()
            db = self.get_db_by_attr(root, "name", str(self.name_db))
            if db is None:
                self.status_config = 0
                self.status_config_message = 'В XML файле: %s не найдена необходимая секция: %s'\
                                             % (self.file_name_config, str(self.name_db))
            try:
                self.connectstring = db.get("connectstring")
            except:
                self.status_config = 0
                self.status_config_message = 'Ошибка получения connectstring.'
        else:
            self.status_config = 0

    @staticmethod
    def get_db_by_attr(root, attrib, value):
        """
            Получение атрибута
        """
        #print '***************' ,".//db[@" + attrib + "='" + value + "']"
        result = None
        database = root.find('database')
        if database is not None:
            for itm in database.findall('db'):
                if itm.attrib[attrib] == value:
                    result = itm
        #return root.find(".//db[@" + attrib + "='" + value + "']")
        return result


class MssqlConfig(BaseDBConfig):
    pass


class MssqlDb(object):
    """
        Класс для работы с MSSQL
    """

    cfg = None
    dn_name = 'MSSQL'
    db_connect = None
    db_message = ''
    db_cursor = None

    def __init__(self, mssql_cfg):
        self.cfg = mssql_cfg
        pyodbc.pooling = False
        self.get_connect()

    def get_connect(self):
        """
            Установка подключения к БД MSSQL
        """

        pyodbc.pooling = False
        self.db_connect = None
        self.db_message = ''
        self.db_cursor = None
        if self.cfg.status_config == 0:
            self.db_message = 'Bad config xml file'
        try:
            self.db_connect = pyodbc.connect(self.cfg.connectstring)
            self.db_cursor = self.db_connect.cursor()
        except:
            self.db_message = self.traceback_log('')

    def close_connect(self):
        """
            Закрытие коннекта к БД
        """

        try:
            self.db_connect.close()
            self.db_connect = None
            self.db_cursor = None
        except:
            pass

    def odb_exec(self, sql, params=(), fetch='many', auto_commit=True):
        """
            Выполнение запросов
        """

        pyodbc.pooling = False
        self.db_message = ''
        res = []
        if self.db_connect:
            try:
                self.db_cursor.execute(sql, params)
                #if re.findall('delete|update|execute|insert|DELETE|UPDATE|EXECUTE|INSERT', sql):
                #    fetch = 'none'
                if fetch == 'many':
                    res = self.db_cursor.fetchall()
                if fetch == 'one':
                    res = self.db_cursor.fetchone()
                if auto_commit:
                    self.db_connect.commit()
            except:
                self.db_message = self.traceback_log('')
        else:
            self.db_message = 'Unable to complete network request to host'
        return res

    def commit(self):
        """
            commit транзакции
        """

        return self.db_connect.commit()

    def rollback(self):
        """
            rollback транзакции
        """

        return self.db_connect.rollback()

    def traceback_log(self, message=''):
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
        message = message + '\n' + tb_ + "\n"
        message = self.decode_x_str(message)
        return message

    @staticmethod
    def decode_x_str(text):
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
                    ret += chr(code) + letter_code[2:] #срабатывает в случае последнего символа
                except:
                    ret += '\\x%s' % letter_code
            else:
                #то, что до первого \x
                first = False
                ret += '%s' % letter_code
        return ret


class Mssql(object):
    """
        Класс для выполнения запросов к MSSQL
    """

    __mssql_db = None
    __name_db = None

    message = None
    result = None

    def __init__(self, name_db):
        self.__name_db = name_db
        self.result = 0

    def mssql_connect(self):
        mssql_cfg = MssqlConfig(self.__name_db)
        if mssql_cfg.status_config == 1:
            self.__mssql_db = MssqlDb(mssql_cfg)
            if not self.__mssql_db.db_connect:
                self.message = self.__mssql_db.db_message
                self.result = 2
                self.__mssql_db = None
        else:
            self.message = mssql_cfg.status_config_message
            self.result = 2
            self.__mssql_db = None

    def mssql_execute_sql(self, sql_text, sqlparams=(), auto_commit=True, fetch='many'):
        """
            Выполнение запросов
        """
        db_local = self.__mssql_db
        if db_local:
            res = []
            try:
                res = db_local.odb_exec(sql_text, params=sqlparams,
                                        fetch=fetch,
                                        auto_commit=auto_commit)
                if db_local.db_message != '':
                    self.result = 2
                    self.message = db_local.db_message
                    return {'status': 3, 'message': db_local.db_message, 'datalist': None}
                else:
                    return {'status': 1, 'message': None, 'datalist': res}
            except Exception as exc:
                self.result = 2
                error = 'Error execute SQL in MSSQL command: %(sql)s %(sqlparams)s, %(err)s'\
                        % {'sql': sql_text, 'sqlparams': sqlparams, 'err': db_local.db_message}
                self.message = error
                return {'status': 2, 'message': error, 'datalist': None}
        else:
            self.result = 2
            error = 'Execute SQL command with lost connections. Unable to complete network request to host.'
            self.message = error
            return {'status': 3, 'message': error, 'datalist': None}
