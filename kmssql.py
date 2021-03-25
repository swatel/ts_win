# -*- coding: cp1251-*
"""
    swat 20.11.2014
    version 0.0.2.2
    ������ ������ � �� MS SQL
"""

import re
import pyodbc

import kdb
import krconst as c
import rbsqutils as rqu


class MssqlConfig(kdb.BaseDBConfig):
    pass


class MssqlDb(object):
    """
        ����� ��� ������ � MSSQL
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
            ��������� ����������� � �� MSSQL
        """

        pyodbc.pooling = False
        self.db_connect = None
        self.db_message = ''
        self.db_cursor = None
        if self.cfg.status_config == c.kr_status_config_error:
            self.db_message = c.kr_message_error_badconfig
        try:
            self.db_connect = pyodbc.connect(self.cfg.connectstring)
            self.db_cursor = self.db_connect.cursor()
        except:
            self.db_message = rqu.TracebackLog('')

    def close_connect(self):
        """
            �������� �������� � ��
        """

        try:
            self.db_connect.close()
            self.db_connect = None
            self.db_cursor = None
        except:
            #todo ������� ����� � ��� ����
            pass

    def odb_exec(self, sql, params=(), fetch='many', auto_commit=True):
        """
            ���������� ��������
        """

        pyodbc.pooling = False
        self.db_message = ''
        res = []
        if self.db_connect:
            try:
                self.db_cursor.execute(sql, params)
                if re.findall('delete|update|execute|insert|DELETE|UPDATE|EXECUTE|INSERT', sql):
                    fetch = 'none'
                if fetch == 'many':
                    res = self.db_cursor.fetchall()
                if fetch == 'one':
                    res = self.db_cursor.fetchone()
                if auto_commit:
                    self.db_connect.commit()
            except:
                self.db_message = rqu.TracebackLog('')
        else:
            self.db_message = 'Unable to complete network request to host'
        return res
    
    def commit(self):
        """
            commit ����������
        """

        return self.db_connect.commit()

    def rollback(self):
        """
            rollback ����������
        """

        return self.db_connect.rollback()
