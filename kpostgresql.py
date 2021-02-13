# -*- coding: utf-8 -*-
"""
    ivaikin.t 11.01.2017
    version 0.0.3.0
    Модуль работы с СУБД PostgreSQL
"""

import re
import psycopg2
import psycopg2.extras as psyextras

import kdb
import krconst as c
import rbsqutils as rqu

from decimal import *

class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)

class DictComposite(psycopg2.extras.CompositeCaster):
    def make(self, values):
        return dict(list(zip(self.attnames, values)))



class PostgresqlConfig(kdb.BaseDBConfig):
    pass

class PostgresqlDb(object):
    """
        Класс для работы с MSSQL
    """

    cfg = None
    dn_name = 'POSTGRESQL'
    db_connect = None
    db_message = ''
    db_cursor = None

    def __init__(self, postgresql_cfg):
        self.cfg = postgresql_cfg
        #pyodbc.pooling = False
        self.get_connect()

    def get_connect(self):
        """
            Установка подключения к БД MSSQL
        """

        #pyodbc.pooling = False
        self.db_connect = None
        self.db_message = ''
        self.db_cursor = None
        if self.cfg.status_config == c.kr_status_config_error:
            self.db_message = c.kr_message_error_badconfig
        try:
            #self.db_connect = pyodbc.connect(self.cfg.connectstring)
            self.db_connect = psycopg2.connect(self.cfg.connectstring, cursor_factory=psyextras.DictCursor)
            self.db_cursor = self.db_connect.cursor(cursor_factory=psycopg2.extras.DictCursor)
            psycopg2.extras.register_composite('card', self.db_cursor, factory=DictComposite)
        except:
            self.db_message = rqu.TracebackLog('')

    def close_connect(self):
        """
            Закрытие коннекта к БД
        """

        try:
            self.db_connect.close()
            self.db_connect = None
            self.db_cursor = None
        except:
            # todo сделать вывод в лог файл
            pass

    def odb_exec(self, sql, params=(), fetch='many', auto_commit=True):
        """
            Выполнение запросов
        """

        #pyodbc.pooling = False
        self.db_message = ''
        res = []
        if self.db_connect:
            try:
                self.db_cursor.execute(sql, tuple(params))
                #print params
                #self.db_cursor.execute(sql, ('2017-01-12','P','31'))
                #self.db_cursor.execute('SELECT (round(sum(amount), 2)) as SUMMA          FROM public.erpi_purchase h     WHERE h.operday = \'2017-01-12 00:00:00.00\'     and \'P\' is not null     and h.shop = 31')
                if re.findall('delete|update|execute|insert|DELETE|UPDATE|EXECUTE|INSERT', sql):
                    fetch = 'none'
                if fetch == 'many':
                    res = self.db_cursor.fetchall()
                    for j in range(0,len(res)):
                        # Исправляем ошибку
                        num_fields = len(self.db_cursor.description)
                        field_names = [i[0] for i in self.db_cursor.description]
                        res1 = {}
                        for i in range(0, len(field_names)):
                            res1[field_names[i].upper()] = res[j][i]
                        res[j] = res1
                        for i in field_names:
                            i = i.upper()
                            if isinstance(res[j][i], Decimal):
                                res[j][i] = float(res[j][i])
                            if isinstance(res[j][i], int):
                                res[j][i] = int(res[j][i])
                        res[j] = Struct(**res[j])
                if fetch == 'one':
                    res = self.db_cursor.fetchone()
                    # Исправляем ошибку
                    num_fields = len(self.db_cursor.description)
                    field_names = [i[0] for i in self.db_cursor.description]
                    res1 = {}
                    for i in range(0, len(field_names)):
                        res1[field_names[i].upper()] = res[i]
                    res = res1
                    for i in field_names:
                        i = i.upper()
                        if isinstance(res[i], Decimal):
                            res[i] = float(res[i])
                        if isinstance(res[i], int):
                            res[i] = int(res[i])
                    res = Struct(**res)
                if auto_commit:
                    self.db_connect.commit()
            except:
                self.db_message = rqu.TracebackLog('')
        else:
            self.db_message = 'Unable to complete network request to host'
        #print res
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
