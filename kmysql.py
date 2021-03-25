# -*- coding: cp1251-*
"""
    swat 20.11.2014
    version 0.0.2.2
    Модуль работы с БД mysql
"""

import re
import mysql.connector
from mysql.connector import errorcode
import kdb
import krconst as c
import rbsqutils as rqu


class MysqlConfig(kdb.BaseDBConfig):
    pass


class MysqlDb(object):
    """
        Класс для работы с MySQL
    """

    cfg = None
    dn_name = 'MySQL'
    db_connect = None
    db_message = ''
    db_cursor = None

    def __init__(self, cfg):
        self.cfg = cfg
        self.get_connect()

    def get_connect(self):
        """
            Подключение к БД MySQL
        """

        mysql_cfg = {}
        if self.cfg.status_config == c.kr_status_config_error:
            self.db_message = c.kr_message_error_badconfig
        else:
            try:
                mysql_cfg = eval(self.cfg.connectstring)
            except:
                self.cfg.status_config = c.kr_status_config_error
                self.db_message = "Ошибка конвертации параметров в dict."
        if self.cfg.status_config == c.kr_status_config_ok:
            try:
                self.db_connect = mysql.connector.connect(**mysql_cfg)
                self.db_connect._connection_timeout = 0

                self.db_cursor = self.db_connect.cursor(named_tuple=True)
                self.db_cursor.execute('SET NAMES cp1251;')
                self.db_cursor.execute('SET CHARACTER SET cp1251;')
                self.db_cursor.execute('SET character_set_connection=cp1251;')

            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    self.db_message = "Неверный логин или пароль."
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    self.db_message = "БД не существует."
                else:
                    self.db_message = err

                '''except MySQLdb.Error as err:
                    self.db_message = err
                    self.db_connect.close()
                '''

    def close_connect(self):
        """
            Закрытие коннекта к БД
        """

        try:
            self.db_cursor.close()
            self.db_connect.close()
            self.db_connect = None
            self.db_cursor = None
        except:
            #todo сделать вывод в лог файл
            pass

    def odb_exec(self, sql, params=(), fetch='many', auto_commit=True):
        """
            Выполнение запросов
        """

        self.db_message = ''
        res = []
        if self.db_connect:
            try:
                res = self.db_cursor.execute(sql, params)
                if re.findall('delete|update|execute|insert|DELETE|UPDATE|EXECUTE|INSERT', sql):
                    fetch = 'none'
                if fetch == 'many':
                    res = self.db_cursor.fetchall()
                if fetch == 'one':
                    res = self.db_cursor.fetchone()
                if auto_commit:
                    self.db_connect.commit()
            except mysql.connector.InterfaceError as err:
                self.db_message = rqu.TracebackLog(err)
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