# -*- coding: utf-8 -*-

"""
    LiteBox
    Экспорт актов и счетов
"""

import os
import json
import time
import datetime

import krconst as c
import BasePlugin as Bp
import kconfig as conf
import queue_db as db

from rbsqutils import decodeUStr
# from rbsqutils import convToWin
from rbsqutils import current_time_zone


__author__ = 'swat'
VERSION = '0.0.3.2'
DATE_VERSION = '03.02.2016'


class Plugin(Bp.BasePlugin):
    """
        LiteBox
    Экспорт актов и счетов

    """

    odb = None
    dir_export = None
    date_beg = None
    date_end = None
    doc_text_end_license = []
    re_write = None

    def run(self):
        """
        Запуск
        """

        self.doc_text_end_license = []

        # получим параметры из БД
        if not self.taskparamsxml and self.taskparamsxml == '':
            self.result['result'] = c.plugin_error
            self.log_file(c.m_e_emptytaskparams,
                          terms = 2,
                          save_log_db=True)
            return False

        # получим директорию экспорта
        self.dir_export = self.parser_xml(self.taskparamsxml, 'dir_export')
        if self.dir_export is None:
            self.result['result'] = c.plugin_error
            self.log_file('Не указана директория экспорта',
                          terms = 2,
                          save_log_db=True)
            return False

        # self.dir_export = self.dir_export.decode('cp1251')

        # Получим период экспорта, если нет, то по умолчанию выгружаем предыдущий день
        self.date_beg = self.parser_xml(self.queueparamsxml, 'date_beg')
        self.date_end = self.parser_xml(self.queueparamsxml, 'date_end')
        self.re_write = self.parser_xml(self.queueparamsxml, 're_write')
        if self.re_write is None:
            self.re_write = '0'

        # получим подключение к БД Engine
        self.log_file('Подключение к Engine', terms=1)
        # engine_conf = conf.KConfig('ENGINE_LITEBOX_SUN')
        engine_conf = conf.KConfig('ENGINE_LITEBOX')
        engine_conf.get_config_file()
        engine_conf.get_config_layer()
        engine_conf.get_config()
        db_engine = db.QueryDB(engine_conf)
        # todo Щеглов Сделать логирование
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
                        # Проверка часового пояса
                        tz = current_time_zone(db_conn, layer_code)
                        if tz <= -4:
                            self.export_doc(db_conn, layer_code, email, sa_uid, login, 2)
                        else:
                            self.export_doc(db_conn, layer_code, email, sa_uid, login, 1)
                if self.doc_text_end_license:
                    now = datetime.datetime.now()
                    file_name = 'dmainlicinse_%s.json' % (time.strftime('%Y%m%d%H%M%S',
                                                                        time.localtime()) + str(now.microsecond))
                    self.export_file(file_name, self.doc_text_end_license)

        else:
            self.log_file('Ошибка подключения к Engine', terms=1)

    def export_doc(self, db_conn, layer_code, email, sa_uid, login, date_add):
        """
        экспорт счетов, актов, оплат
        @param db_conn: подключение к слою
        @param layer_code: код слоя
        @param email: email
        @param login: login
        @param sa_uid: id пользователя
        @param date_add: сколько дней отнять
        """

        # выгрузка счетов
        sql_text = '''select h.invoicehistoryid, datetostr(h.ihdate,'%d.%m.%Y %H:%M:%S') as ihdate,
                             h.number, h.type_invoice, h.amount, padleft(h.uid, 9, '0') as uid,
                             o.comp_inn, sp.price as service_tarif_price, st.name as service_tarif_name
                        from invoice_history h
                             left join my_get_ownercompany_info o on 1=1
                             left join my_service_doc_position sp on h.SERVICEDOCID = sp.SERVICEDOCID
                             left join my_service_tarif st on sp.servicetarifid = st.serviceid
                       where h.type_invoice = 1
                         and h.ihdate < coalesce(?, current_date) '''
        sql_text += 'and h.ihdate >= coalesce(?, current_date - %s)' % date_add

        res_sql = self.execute_sql(sql_text,
                                   sql_params=[self.date_beg, self.date_end],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения счетов' + c.kr_term_enter)
        else:
            result = res_sql['datalist']
            if result:
                for doc in result:
                    doc_txt = {'invoicehistoryid': doc['invoicehistoryid'],
                               'ihdate': doc['ihdate'],
                               'number': doc['number'],
                               'type_invoice': doc['type_invoice'],
                               'amount': doc['amount'],
                               'uid': sa_uid,
                               'email': email,
                               'login': login,
                               'inn': doc['comp_inn'],
                               'service_tarif_name': doc['service_tarif_name'],
                               'service_tarif_price': doc['service_tarif_price']
                               }
                    file_name = 'invoice_' + layer_code + '_' + str(doc['invoicehistoryid']) + '.json'
                    self.export_file(file_name, doc_txt)

        # выгрузка актов
        sql_text = '''select b.balancehistoryid, b.balancebefore, b.balancenow,
                             datetostr(b.createtime, '%d.%m.%Y %H:%M:%S') as createtime,
                             datetostr(b.bhdate, '%d.%m.%Y %H:%M:%S') as bhdate,
                             b.typeaction, b.countlicense, b."TYPE", b.nummonth, b.pricetoday,
                             b.numact, o.comp_inn,
                             datetostr(p.activetodate, '%d.%m.%Y %H:%M:%S') as activetodate,
                             datetostr(b.dbegperiod, '%d.%m.%Y') as dbegperiod,
                             datetostr(b.dendperiod, '%d.%m.%Y') as dendperiod,
                             datetostr(tc.dmain_licinse, '%d.%m.%Y %H:%M:%S') as dmainlicinse,
                             case when pp.dbeg is not null then datetostr(pp.dbeg, '%d.%m.%Y') else null end as dbeg,
                             pp.amountdays, sp.price as service_tarif_price, st.name as service_tarif_name, b.bonus
                        from BALANCE_HISTORY b
                             left join my_get_ownercompany_info o on 1=1
                             left join (select first 1 activetodate from profile where is_owner = 1) p on 1=1
                             left join MY_PROMISED_PAYMENT pp on pp.id = b.promisedpaymentid
                             left join TARIF_CONFIG tc on 1=1
                             left join my_service_doc_position sp on b.servicedocid = sp.SERVICEDOCID
                             left join my_service_tarif st on sp.servicetarifid = st.serviceid
                       where b.type = -1
                         and b.bhdate < coalesce(?, current_date) '''
        sql_text += 'and b.bhdate >= coalesce(?, current_date - %s)' % date_add

        res_sql = self.execute_sql(sql_text,
                                   sql_params=[self.date_beg, self.date_end],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения актов' + c.kr_term_enter)
        else:
            result = res_sql['datalist']
            if result:
                for doc in result:
                    doc_txt = {'balancehistoryid': doc['balancehistoryid'],
                               'balancebefore': doc['balancebefore'],
                               'balancenow': doc['balancenow'],
                               'createtime': doc['createtime'],
                               'bhdate': doc['bhdate'],
                               'typeaction': doc['typeaction'],
                               'countlicense': doc['countlicense'],
                               'type': doc['TYPE'],
                               'nummonth': doc['nummonth'],
                               'pricetoday': doc['pricetoday'],
                               'numact': doc['numact'],
                               'uid': sa_uid,
                               'email': email,
                               'login': login,
                               'inn': doc['comp_inn'],
                               'activetodate': doc['activetodate'],
                               'dmainlicinse': doc['dmainlicinse'],
                               'dbegperiod': doc['dbegperiod'],
                               'dendperiod': doc['dendperiod'],
                               'dbeg': doc['dbeg'],
                               'amountdays': doc['amountdays'],
                               'service_tarif_name': doc['service_tarif_name'],
                               'service_tarif_price': doc['service_tarif_price'],
                               'bonus': doc['bonus']
                               }
                    file_name = 'balance_' + layer_code + '_' + str(doc['balancehistoryid']) + '.json'
                    self.export_file(file_name, doc_txt)

        # выгрузка оплат
        sql_text = '''select b.balancehistoryid, b.balancebefore, b.balancenow,
                             datetostr(b.createtime, '%d.%m.%Y %H:%M:%S') as createtime,
                             datetostr(b.bhdate, '%d.%m.%Y %H:%M:%S') as bhdate,
                             b.typeaction, b.countlicense, b."TYPE", b.nummonth, b.pricetoday,
                             b.numact, o.comp_inn
                        from BALANCE_HISTORY b
                             left join my_get_ownercompany_info o on 1=1
                       where b.type = 1
                         and b.typeaction = 'Robokassa'
                         and b.bhdate < coalesce(?, current_date) '''
        sql_text += 'and b.bhdate >= coalesce(?, current_date - %s)' % date_add

        res_sql = self.execute_sql(sql_text,
                                   sql_params=[self.date_beg, self.date_end],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения оплат' + c.kr_term_enter)
        else:
            result = res_sql['datalist']
            if result:
                for doc in result:
                    doc_txt = {'balancehistoryid': doc['balancehistoryid'],
                               'balancebefore': doc['balancebefore'],
                               'balancenow': doc['balancenow'],
                               'createtime': doc['createtime'],
                               'bhdate': doc['bhdate'],
                               'typeaction': doc['typeaction'],
                               'countlicense': doc['countlicense'],
                               'type': doc['TYPE'],
                               'nummonth': doc['nummonth'],
                               'pricetoday': doc['pricetoday'],
                               'numact': doc['numact'],
                               'uid': sa_uid,
                               'email': email,
                               'login': login,
                               'inn': doc['comp_inn']
                               }
                    file_name = 'balance_pay_' + layer_code + '_' + str(doc['balancehistoryid']) + '.json'
                    self.export_file(file_name, doc_txt)

        # выгрузка начисления бонусов
        sql_text = '''select b.balancehistoryid, b.balancebefore, b.balancenow,
                             datetostr(b.createtime, '%d.%m.%Y %H:%M:%S') as createtime,
                             datetostr(b.bhdate, '%d.%m.%Y %H:%M:%S') as bhdate,
                             b.typeaction, b.countlicense, b."TYPE", b.nummonth, b.pricetoday,
                             b.numact, o.comp_inn
                        from BALANCE_HISTORY b
                             left join my_get_ownercompany_info o on 1=1
                       where b.type = 1
                         and b.typeaction = 'LiteboxBonus'
                         and b.bhdate < coalesce(?, current_date) '''
        sql_text += 'and b.bhdate >= coalesce(?, current_date - %s)' % date_add

        res_sql = self.execute_sql(sql_text,
                                   sql_params=[self.date_beg, self.date_end],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта начисления бонусов' + c.kr_term_enter)
        else:
            result = res_sql['datalist']
            if result:
                for doc in result:
                    doc_txt = {'balancehistoryid': doc['balancehistoryid'],
                               'balancebefore': doc['balancebefore'],
                               'balancenow': doc['balancenow'],
                               'createtime': doc['createtime'],
                               'bhdate': doc['bhdate'],
                               'typeaction': doc['typeaction'],
                               'countlicense': doc['countlicense'],
                               'type': doc['TYPE'],
                               'nummonth': doc['nummonth'],
                               'pricetoday': doc['pricetoday'],
                               'numact': doc['numact'],
                               'uid': sa_uid,
                               'email': email,
                               'login': login,
                               'inn': doc['comp_inn']
                               }
                    file_name = 'balance_bonus' + layer_code + '_' + str(doc['balancehistoryid']) + '.json'
                    self.export_file(file_name, doc_txt)
                                
        # выгрузка даты окончания лицензии
        sql_text = '''select o.comp_inn, datetostr(tc.dmain_licinse, '%d.%m.%Y %H:%M:%S') as dmainlicinse
                        from TARIF_CONFIG tc
                             left join my_get_ownercompany_info o on 1=1'''

        res_sql = self.execute_sql(sql_text,
                                   sql_params=[],
                                   fetch='many',
                                   db_local=db_conn)
        if res_sql['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения скрипта получения даты окончания лицензии' + c.kr_term_enter)
        else:
            result = res_sql['datalist']
            if result:
                for doc in result:
                    self.doc_text_end_license.append({'uid': sa_uid,
                                                      'email': email,
                                                      'login': login,
                                                      'inn': doc['comp_inn'],
                                                      'dmainlicinse': doc['dmainlicinse']
                                                      })

    def export_file(self, file_name, doc_txt):
        """
        Создание файла
        @param file_name: имя файла
        @param doc_txt: dict документа
        """

        file_name = os.path.join(self.dir_export, file_name)
        # file_name = file_name.decode('cp1251')
        # file_name = convToWin(file_name, encoding='utf-8', errors='replace')
        # print 'file_name', file_name
        self.log_file('Сохраняем файл:' + file_name, terms=1)
        if self.re_write == '0':
            if self.exists_file(file_name.decode('cp1251'), add_log=False):
                self.log_file('Файл существует, не перезаписываем его:' + file_name, terms=1)
                return False

        self.log_file('Создаем файл:' + file_name, terms=1)
        self.delete_tmp_file(file_name.decode('cp1251'))
        json_result = decodeUStr(json.dumps(doc_txt, encoding='cp1251', indent=1).encode('cp1251'))
        file_save = open(file_name.decode('cp1251'), "a")
        print(json_result, file=file_save)
        file_save.close()
        self.log_file('Сохранили.', terms=1)
