# -*- coding: utf-8 -*-
# swat 14.01.2014
# version 0.0.2.0

"""
    класс импорта пользователей
"""

import re
import krconst
from rbsqutils import translit_to_ident


class User():
    """
        класс импорта пользователей
    """

    parent_class = None
    class_name = None

    user_id = None
    man_id = None
    dolgnid = None
    name_user = None
    type_work = None
    login = None
    result_class = krconst.plugin_ok

    _cnt_user_engine = None

    connect_engine = None

    def __init__(self, parent_class, man_id, name_user,
                 user_id, dolgnid, connect_engine):
        """
            Инициализация перменных
        """

        self.parent_class = parent_class
        self.class_name = type(self).__name__

        self.man_id = man_id
        self.name_user = name_user
        self.user_id = user_id
        self.dolgnid = dolgnid

        self.connect_engine = connect_engine

    def save(self):
        """
            Сохранение пользователя в БД
        """

        # проверим привязано ли физ лицо к пользователям RBS
        self._check_rbs()
        if self.result_class == krconst.plugin_ok:
            # если id_user = None, то пользователь
            # еще не привязан или не существует
            # проверим есть ли он в ENGINE имени
            if self.user_id:
                # Обновляем
                self._save_rbs()
            else:
                self._user_check_engine()
                # поверим сколько нашлось пользователь по ФИО,
                # > 1  то привязку делать не будем генерируется ошибка
                # = 0 добавляем
                # = 1 обновляем

                if self.result_class == krconst.plugin_ok:
                    if self._cnt_user_engine == 0:
                        #добавляем
                        self._generate_login()
                        self._user_save_engine()
                        if self.user_id:
                            self._save_rbs()
                    if self._cnt_user_engine == 1:
                        # Обновляем
                        self._save_rbs()

    def _check_rbs(self):
        """
            Проверка привязан ли пользователь к физ лицу
        """

        self.type_work = 'S'

        self._check_save()

    def _save_rbs(self):
        """
            Проверка привязан ли пользователь к физ лицу
        """

        self.type_work = 'I'

        self._check_save()

    def _check_save(self):
        """
            Привязка пользователя к физ лицу
        """

        sql_text = 'select * from RBS_Q_USER_INSSEL(?,?,?,?,?)'
        sql_params = [self.man_id, self.name_user,
                      self.user_id, self.type_work,
                      self.dolgnid]

        res = self.parent_class.ExecuteSQL(sql_text,
                                           sqlparams = sql_params,
                                           fetch='one',
                                           ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            message = krconst.m_e_importcheckuser % self.name_user
            self.parent_class.log_file(message,
                                       terms=2,
                                       save_log_db=True)
            self.result_class = krconst.plugin_error
        else:
            if res['datalist']:
                self.user_id = res['datalist']['ID_USER']

    def _user_check_engine(self):
        """
            Проверка прльзователя в БД Engine
        """

        sql_text = '''select u.id_user, (select count(u1.id_user) as cntusers
                                          from engine_users u1
                                         where (UPPER(u1.FIO) = UPPER(?)) )
                       from ENGINE_USERS u
                      where (UPPER(u.FIO) = UPPER(?))'''
        sql_params = [self.name_user, self.name_user]

        engine_user = self.parent_class.ExecuteSQL(sql_text,
                                                   sqlparams=sql_params,
                                                   fetch='one',
                                                   db_local=self.connect_engine,
                                                   ExtVer=True)
        if engine_user['status'] == krconst.kr_sql_error:
            message = krconst.m_e_importcheckuserengine % self.name_user
            self.parent_class.log_file(message,
                                       terms=2,
                                       save_log_db=True)
            self.result_class = krconst.plugin_error
        else:
            if engine_user['datalist']:
                self._cnt_user_engine = engine_user['datalist']['cntusers']
                if self._cnt_user_engine > 1:
                    self.parent_class.result['result'] = krconst.plugin_ok
                    message = krconst.m_e_importuserenginecnt % self.name_user
                    self.parent_class.log_file(message,
                                               terms=2,
                                               save_log_db=True)
                    self.result_class = krconst.plugin_error
                if self._cnt_user_engine == 1:
                    self.user_id = engine_user['datalist']['id_user']
            else:
                self._cnt_user_engine = 0

    def _generate_login(self):
        """
            Генерация логина
        """

        re_search = re.search(r'([\S]+)\s+(.+)', self.name_user)
        if re_search is None:
            f_1 = self.name_user
            i_1 = ''
            o_1 = ''
        else:
            f_1 = re_search.group(1)
            io_1 = re_search.group(2)
            if f_1 is None or io_1 is None:
                f_1 = self.name_user
                i_1 = ''
                o_1 = ''
            else:
                re_search_io = re.search(r'([\S]+)\s+(.+)', io_1)
                if re_search_io is None:
                    i_1 = io_1
                    o_1 = ''
                else:
                    i_1 = re_search_io.group(1)
                    o_1 = re_search_io.group(2)
                    if i_1 is None or o_1 is None:
                        i_1 = io_1
                        o_1 = ''
        if len(i_1) > 0:
            i_1 = i_1[0]
        if len(o_1) > 0:
            o_1 = o_1[0]
        login = translit_to_ident(f_1, trunc_punctuation=True)
        login = login + translit_to_ident(i_1, trunc_punctuation=True)
        login = login + translit_to_ident(o_1, trunc_punctuation=True)
        self.login = login

    def _user_save_engine(self):
        """
            Добавление пользователя в БД Engine
        """

        sql_text = 'select * from ENGINE_USER_ADD(?,?,?,?,?,?,?,?,?)'
        sql_params = [None, None, self.login,
                      self.name_user, None,
                      None, None, None, None]
        res = self.parent_class.ExecuteSQL(sql_text,
                                           sqlparams=sql_params,
                                           fetch='one',
                                           db_local=self.connect_engine,
                                           ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            message = krconst.m_e_importadduserengine % self.name_user
            self.parent_class.log_file(message,
                                       terms=2,
                                       save_log_db=True)
            self.result_class = krconst.plugin_error
        else:
            if res['datalist']:
                if res['datalist']['ERROR_MSG']:
                    self.parent_class.log_file(res['datalist']['ERROR_MSG'],
                                               terms=2,
                                               save_log_db=True)
                    self.parent_class.result['result'] = krconst.plugin_error
                    self.result_class = krconst.plugin_error
                else:
                    self.user_id = res['datalist']['OUT_ID_USER']