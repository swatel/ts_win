# -*- coding: utf-8 -*-
"""
    Elba Kontur
    Экспорт z-отчетов
"""

import os
import json
import time
import datetime
import requests

import krconst as c
import BasePlugin as Bp
import kconfig as conf
import queue_db as db
import rbsqutils as pu
from requests.packages.urllib3.poolmanager import PoolManager
import ssl


__author__ = 'kast'
VERSION = '0.0.0.1'
DATE_VERSION = '28.05.2017'


class Tls1HttpAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLSv1
        )


class Plugin(Bp.BasePlugin):

    url = 'https://mario.testkontur.ru/payments/import'
    # url = 'https://elba.kontur.ru/payments/import'
    headers = {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache'
    }
    # exchange_task_id = None
    test_inn = '195256565714'
    api_key = None
    exchange_code = 'KONTUR'

    def run(self):
        """
        Запуск
        """

        # получим параметры обмена
        res = self.execute_sql(
            'select * from Q_API_GETEXCHANGETASK(?,?,?)',
            sql_params=[None, self.exchange_code, None],
            fetch='one'
        )
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения параметров задачи обмена', terms=1)
            return False
        else:
            row = res['datalist']
            params = json.loads(str(row['exchangeparams']), encoding='cp1251')
            self.api_key = params['api_key']


        # получим подключение к БД Engine
        self.log_file('Подключение к Engine', terms=1)
        # engine_conf = conf.KConfig('ENGINE_LITEBOX_SUN')
        engine_conf = conf.KConfig('ENGINE_LITEBOX')
        engine_conf.get_config_file()
        engine_conf.get_config_layer()
        engine_conf.get_config()
        db_engine = db.QueryDB(engine_conf)
        if db_engine.connect:
            self.log_file('Подклчение к Engine прошло успешно', terms=1)
            # Получаем все слои и дополнительные параметры

            sql_text = '''select l.code, u.email,  LPAD(u.sa_uid, 9, '0') as sa_uid, u.login
                                   from engine_layers l
                                        left join engine_users u on l.owner_id = u.id_user
                                   where l.code <> 'GLOBAL' '''
            res = self.execute_sql(sql_text,
                                   sql_params=[],
                                   db_local=db_engine,
                                   fetch='many'
                                   )
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения всех слоев', terms=1)
            else:
                for itm in res['datalist']:
                    db_conn = None
                    layer_code = itm['code']
                    email = itm['email']
                    sa_uid = itm['sa_uid']
                    login = itm['login']
                    self.log_file('Подключаемся слою:' + layer_code, terms=1)
                    # получаем подключение к БД слоя
                    k_conf = conf.KConfig(layer_code)
                    k_conf.get_os_version()
                    k_conf.get_config_file()
                    k_conf.get_config(layer_code, engine_conf)
                    db_conn = db.QueryDB(k_conf)
                    if db_conn is None:
                        self.log_file('Ошибка подключения к слою ' + layer_code, terms=1)
                    else:
                        self.log_file('Подключение прошло успешно', terms=1)
                        # проверка разрешения экспорта в kontur на весь слой
                        if self.check_access_export(db_conn):
                            # получение организаций в слое
                            organisation = self.get_organisation(db_conn)
                            if organisation:
                                for org in organisation:
                                    orgid = org['OBJID']
                                    org_inn = org['INN']
                                    # проверка разрешения экспорта в kontur на организацию
                                    if self.check_access_export_org(db_conn, orgid):
                                        # check date's
                                        dates = self.get_dates(db_conn, orgid)
                                        for item in dates:
                                            report = self.get_report_by_date(db_conn, orgid, item['REPORTDATE'])
                                            if report:
                                                data = {'ApiKey': self.api_key,
                                                        'Inn': self.test_inn, #org_inn,
                                                        'Payments': self.format_report(report)}
                                                try:
                                                    # data = json.dumps(data, encoding='utf-8')

                                                    req = requests.Session()
                                                    # req.cert = 'path to *.cer'
                                                    req.mount('https://', Tls1HttpAdapter())
                                                    r = req.post(self.url, verify=True) #headers=self.headers
                                                    if r.status_code == 200:
                                                        self.exchange_success(db_conn, orgid, item['REPORTDATE'])
                                                    print(r)
                                                except Exception as exc:
                                                    print(exc)

                        else:
                            self.log_file('Запрет на экспорт в слое ' + layer_code, terms=1)
                        # Проверка часового пояса
                        # tz = current_time_zone(db_conn, layer_code)
                        # if tz <= -4:
                        #     self.export_doc(db_conn, layer_code, email, sa_uid, login, 2)
                        # else:
                        #     self.export_doc(db_conn, layer_code, email, sa_uid, login, 1)
                # if self.doc_text_end_license:
                #     now = datetime.datetime.now()
                #     file_name = 'dmainlicinse_%s.json' % (time.strftime('%Y%m%d%H%M%S',
                #                                                         time.localtime()) + str(now.microsecond))
                #     self.export_file(file_name, self.doc_text_end_license)

        else:
            self.log_file('Ошибка подключения к Engine', terms=1)

    def format_report(self, report):
        data = []
        for item in report:
            try:
                data.append({
                    'Number': item['SESSIONID'],
                    'Date': pu.formatMxDateTime(item['REPDATE'], '%d.%m.%Y'),
                    'SumCash': item['SUMCASH'],
                    'SumEMoney': item['SUMEMONEY'],
                    'CashboxNumber': pu.convToUTF8(item['EQUIPMENTNAME']),
                    'CashierName': pu.convToUTF8(item['FIO']),
                    'ShopName': pu.convToUTF8(item['SHOPNAME']),
                })
            except Exception as exc:
                print(exc)
        return data

    def get_organisation(self, db_conn):  # TODO check INN if is null ?
        sql_text = "select * \
                      from my_spobjects_get(null, (select first 1 CATID from CATEGORY where CODE = 'ORGANIZ'), null)"
        res_sql = self.execute_sql(sql_text,
                                   sql_params=[],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения организаций' + c.kr_term_enter)
            return []
        else:
            return res_sql['datalist']

    def get_report_by_date(self, db_conn, orgid, date):
        sql_text = 'select * from Q_KONTUR_REPORTS(?, ?)'
        res_sql = self.execute_sql(sql_text,
                                   sql_params=[orgid, date],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения z-отчета' + c.kr_term_enter)
            return []
        else:
            return res_sql['datalist']

    def get_dates(self, db_conn, orgid):
        sql_text = 'select * from Q_KONTUR_OBJDATES(?)'  # TODO rename
        res_sql = self.execute_sql(sql_text,
                                   sql_params=[orgid],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения дат на экспорт' + c.kr_term_enter)
            return []
        else:
            return res_sql['datalist']

    def check_access_export(self, db_conn):
        is_access = False
        sql_text = 'select q.EXPORT_ZREPORT_KONTUR from RBS_Q_CONFIG q'
        res_sql = self.execute_sql(sql_text,
                                   sql_params=[],
                                   fetch='one',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения разрешения на экспорт слоя' + c.kr_term_enter)
        else:
            is_access = True if res_sql['datalist']['export_zreport_kontur'] == 1 else False
        return is_access

    def check_access_export_org(self, db_conn, objid):
        is_access = False
        sql_text = 'select c.export_zreport_kontur\
                      from company c\
                     where c.compid = ?'
        res_sql = self.execute_sql(sql_text,
                                   sql_params=[objid],
                                   fetch='one',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения разрешения на экспорт организации' + c.kr_term_enter)
        else:
            is_access = True if res_sql['datalist']['export_zreport_kontur'] == 1 else False
        return is_access

    def exchange_success(self, db_conn, objid, date):
        """
        Установка даты/времени последнего успешного обмена c kontur
        """
        # Обновим
        sql_text = 'execute procedure Q_KONTUR_EXCHANGETASKSUCCESS(?,?)'
        sql_params = [objid, date]
        res = self.execute_sql(sql_text, sql_params=sql_params, fetch='none', db_local=db_conn)
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка при обновлении даты последнего успешного обмена с kontur ' + c.t_double_enter)
