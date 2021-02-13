# -*- coding: utf-8 -*-

"""
    модуль экспорта данных в json используя ORM
"""

import json
import os
import time
import datetime

import BasePlugin as Bp
import krconst as c
from orm.models import Document as Doc
from orm.models.rarus import Docrarus as M_doc_rarus
from orm.models.rarus import Zreport as M_z_rarus
from orm.models import Docgood as Cargo
from orm.models import Object as M_Obj
from orm.models import Gwares as M_Gwares
from orm.models.Zreport import Zreport
from orm.models.Order import Order
from orm.utils import *
from orm.models.viki import Waresgroup as wiki_Wg
from orm.models.viki import Goods as wiki_Gw
from orm.models.viki import CashUser as wiki_Us
import kconfig as conf
import queue_db as db

from rbsqutils import decodeUStr

__author__ = 'swat'
VERSION = '1.0.0.2'
DATE_VERSION = '28.03.2016'

def local_decodeUStr(s):
    """
    Перекодировка
    @param s:
    @return:
    """
    tmp = str(s, 'cp1251')
    out = ''
    miss = 0
    for p, c in enumerate(tmp):
        if miss:
            miss -= 1
            continue
        if c != '\\':
            out += c
        else:
            try:
                if tmp[p+1] in '"\\/bfnrt':
                    out += tmp[p+1].decode('unicode_escape')
                    miss = 1
                elif tmp[p+1] == 'u':
                    out += tmp[p:p+6].decode('unicode_escape')
                    miss = 5
                else:
                    out += '\\'
            except:
                pass

    return out.encode('cp1251')


class Plugin(Bp.BasePlugin):
    """
        Класс импорта из json
    """

    version = '2.0'
    export_dic = None
    db_local = None
    use_layer_callback = None

    def run(self):
        self.use_layer_callback = False
        try:
            self._run()
            if self.result['result'] == c.plugin_error and self.use_layer_callback:
                self.queue_callback('E', None)
        except:
            if self.use_layer_callback:
                self.queue_callback('E', None)
            raise

    def set_db_locale(self, rule_dic):
        """
        Установка БД
        @param rule_dic:
        @return:
        """
        # todo Щеглов: Нужна проверка при подключении, а то всегда возвращает true
        type_export = rule_dic['type_export']
        if type_export == 'docs1c':
            layer_code = rule_dic['layer_code']
            # получим подключение к БД Engine
            self.log_file('Подключение к Engine', terms=1)
            engine_conf = conf.KConfig('ENGINE_LITEBOX')
            engine_conf.get_config_file()
            engine_conf.get_config_layer()
            engine_conf.get_config()

            # подключимся к слою
            k_conf = conf.KConfig(layer_code)
            k_conf.get_os_version()
            k_conf.get_config_file()
            k_conf.get_config(layer_code, engine_conf)
            self.db_local = db.QueryDB(k_conf)
            if self.db_local is None:
                self.log_file('Ошибка подключения к слою ' + layer_code, terms=1)
                self.result['result'] = c.plugin_error
                return False
            else:
                self.log_file('Подключение к слою прошло успешно ' + layer_code, terms=1)
                self.use_layer_callback = True
        return True

    def _run(self):
        """
            Экспорт данных
        """
        # получим параметры экспорта
        if self.ruleparams is not None and self.ruleparams != '':
            self.export_dic = self.xml_get_all_params(self.ruleparams, as_dic=True)
            if self.result['result'] == c.plugin_error:
                return False

        rule_dic = self.xml_get_all_params(self.queueparamsxml, as_dic=True)
        if not self.set_db_locale(rule_dic):
            return False
        type_export = rule_dic['type_export']
        session_ids = []

        if type_export in ['object', 'gwares', 'doc', 'docsale', 'vikimini', 'docs1c']:
            if 'IDs' in rule_dic and rule_dic['IDs'] is not None and rule_dic['IDs'] != '':
                id_list = str(rule_dic['IDs']).split(',')
            else:
                # Получим список id из запроса, который находится в настройках R_EXPORT
                try:
                    sql_text = self.export_dic[type_export + 'sql']
                except KeyError:
                    # todo Щеглов: добавить обработку ошибок
                    return False
                id_list = []
                sql_params = []
                if type_export == 'docsale':
                    obj_id = None
                    date_export = None
                    try:
                        session_id = rule_dic['session_id']
                        session_ids.append(session_id)
                    except KeyError:
                        pass
                    try:
                        obj_id = rule_dic['obj_id']
                    except KeyError:
                        pass
                    try:
                        date_export = rule_dic['date_export']
                    except KeyError:
                        pass

                    sql_params = [session_id, obj_id, date_export]
                elif type_export == 'docs1c':
                    try:
                        date_begin = rule_dic['begintime']
                    except KeyError:
                        date_begin = None
                    try:
                        date_end = rule_dic['endtime']
                    except KeyError:
                        date_end = None
                    try:
                        doctypes = rule_dic['doctypes']
                    except KeyError:
                        doctypes = None
                    try:
                        owner_id = rule_dic['owner_id']
                    except KeyError:
                        owner_id = None
                    try:
                        shop_id = rule_dic['shop_id']
                    except KeyError:
                        shop_id = None
                    if doctypes is not None and doctypes.find('-100') > -1:  # Есть кассовые операции
                        sessions_sql_text = """select sessionid from Q_EXPORT_GET_SESSIONS(?,?,?,?)"""
                        sql_params = [date_begin, date_end, owner_id, shop_id]
                        res = self.execute_sql_local(sessions_sql_text,
                                                     sql_params=sql_params,
                                                     fetch='many')
                        if res['status'] == c.kr_sql_error:
                            self.log_file('Ошибка получения списка сессий кассы для экспорта.' + c.kr_term_enter +
                                          res['message'] if 'message' in res else '')
                            return False
                        else:
                            if res['datalist']:
                                for row in res['datalist']:
                                    session_ids.append(row['sessionid'])
                    sql_params = [date_begin, date_end, doctypes, owner_id, shop_id]

                res = self.execute_sql_local(sql_text,
                                             sql_params=sql_params,
                                             fetch='many')
                if res['status'] == c.kr_sql_error:
                    self.log_file('Ошибка получения результата.' + c.kr_term_enter +
                                  res['message'] if 'message' in res else '')
                    return False
                rows = res['datalist']
                if rows:
                    for row in rows:
                        id_list.append(row['id'])

        # определим тип экспорта
        if type_export in ('doc', 'docsale', 'docs1c'):
            result_docs = []
            for doc_id in id_list:
                # doc_id = rule_dic['IDs']
                if self.db_local is None:
                    self.create_queue_bond(doc_id, 'E')
                sql_text = 'select * from Q_EXP_DOC_V1(?,?)'
                params = [doc_id, 'GLOBAL']
                doc = Doc.Document.gets(self.execute_sql_local, sql_text, params)
                doc_json = doc[0].get_json(execute_sql_func=self.execute_sql_local)

                # связь документов
                sql_docbond = 'select * from Q_EXP_DOC_BOND_V1(?,?)'
                doc_bond = self.execute_sql_local(sql_docbond, sql_params=[doc[0].doc_id, 'GLOBAL'])
                doc_bond_json = []
                # todo Щеглов: добавить обработку ошибок
                for bond in doc_bond['datalist']:
                    doc_bond_json.append({'docid': bond['doc2id'],
                                          'bondcode': bond['bondcode'],
                                          'bondname': bond['bondname'],
                                          'externalid': bond['externalid'],
                                          })
                doc_json['docbond'] = doc_bond_json

                sql_text = 'select * from Q_EXP_CARGO_V1(?,?)'
                params = [doc[0].doc_id, 'GLOBAL']
                cargo_list = Cargo.Docgood.gets(self.execute_sql_local, sql_text, params)
                cargo_json_result = []
                for cargo in cargo_list:
                    cargo_json = cargo.get_json(fk_as_object=True, execute_sql_func=self.execute_sql_local)
                    sql_detail_wgroup = """SELECT wg.wgcode, wg.wgname, wg.levelnumber, wg.externalid, wg.externalcode, wg.waresgr
                                                 FROM Q_EXP_WGROUP(?, 1, NULL, ?) wg"""

                    cargo_wgroup = self.execute_sql_local(sql_detail_wgroup, sql_params=[cargo.wg_id, 'GLOBAL'])
                    wgroup_json = []
                    # todo Щеглов: добавить обработку ошибок
                    for wgroup in cargo_wgroup['datalist']:
                        wgroup_json.append({'wgcode': wgroup['wgcode'],
                                            'waresgrid': wgroup['waresgr'],
                                            'wgname': wgroup['wgname'],
                                            'wglevelnumber': wgroup['levelnumber'],
                                            'wgexternalcode': wgroup['externalcode'],
                                            'wgexternalid': wgroup['externalid']})
                    cargo_json['parent_wgroup'] = wgroup_json
                    # ШК
                    sql_detail_barcode = """SELECT b.waresbarcode FROM Q_EXP_WARESBARCODE_V1(?) b"""

                    cargo_barcode = self.execute_sql(sql_detail_barcode, sql_params=[cargo.wares_id])
                    barcode_json = []
                    for barcode in cargo_barcode['datalist']:
                        if barcode['waresbarcode'] is not None:
                            barcode_json.append({'barcode': barcode['waresbarcode']})
                    cargo_json['barcodes'] = barcode_json
                    # ЕГАИС
                    sql_detail_egais = """SELECT e.*
                                                 FROM MY_DOC_GOODS_EGAIS_INFO(?) e"""

                    cargo_egais = self.execute_sql_local(sql_detail_egais, sql_params=[int(cargo.cargo_id)])
                    egais_json = []
                    # todo Щеглов: добавить обработку ошибок
                    for egais in cargo_egais['datalist']:
                        if egais['amount'] is not None:
                            egais_json.append({'informbregid': egais['informbregid'],
                                               'informaregid': egais['informaregid'],
                                               'ttninformbregid': egais['ttninformbregid'],
                                               'identity': egais['identity'],
                                               'amount': egais['amount'],
                                               'registregais': egais['egaisregister']})
                    cargo_json['egais'] = egais_json

                    cargo_json_result.append(cargo_json)

                doc_json['cargo'] = cargo_json_result

                result_docs.append({'document': doc_json})

            callback_filename = None
            result_json = []
            if len(result_docs) > 0:
                result_json.append({'documents': result_docs})
            if len(session_ids) > 0:
                orders_json = []
                z_report_json = []
                for session_id in session_ids:
                    z_report = self.get_z_report(session_id)
                    if z_report is not None:
                        z_report_json.append(z_report.get_json(fk_as_object=True,
                                                               execute_sql_func=self.execute_sql_local))
                    orders = self.get_orders(session_id)
                    if len(orders) > 0:
                        for order in orders:
                            orders_json.append(order.get_json(fk_as_object=True,
                                                              execute_sql_func=self.execute_sql_local))
                if len(orders_json) > 0:
                    result_json.append({'zreports': z_report_json})
                if len(orders_json) > 0:
                    result_json.append({'orders': orders_json})
            if len(result_json) > 0:
                if self.create_file(rule_dic, result_json):
                    callback_filename = self.export_file_name
            if self.use_layer_callback:
                if not self.queue_callback('1', callback_filename):
                    return False

        elif type_export == 'gwares':
            result_gwares = []
            for wares_id in id_list:
                # wares_id = rule_dic['IDs']
                wares = M_Gwares.Gwares.get(wares_id, self.execute_sql)
                wares_json = wares.get_json(execute_sql_func=self.execute_sql, fk_as_object=True)

                # группы
                sql_detail_wgroup = """SELECT wg.wgcode, wg.wgname, wg.levelnumber, wg.externalid, wg.externalcode, wg.waresgr
                                             FROM Q_EXP_WGROUP(?, 1, NULL, ?) wg"""

                cargo_wgroup = self.execute_sql(sql_detail_wgroup, sql_params=[wares.wg_id, 'GLOBAL'])
                wgroup_json = []
                # todo Щеглов: добавить обработку ошибок
                for wgroup in cargo_wgroup['datalist']:
                    wgroup_json.append({'wgcode': wgroup['wgcode'],
                                        'waresgrid': wgroup['waresgr'],
                                        'wgname': wgroup['wgname'],
                                        'wglevelnumber': wgroup['levelnumber'],
                                        'wgexternalcode': wgroup['externalcode'],
                                        'wgexternalid': wgroup['externalid']})

                wares_json['parent_wgroup'] = wgroup_json

                # ШК
                sql_detail_barcode = """SELECT b.waresbarcode
                                             FROM Q_EXP_WARESBARCODE_V1(?) b"""

                cargo_barcode = self.execute_sql(sql_detail_barcode, sql_params=[wares.wares_id])
                barcode_json = []
                # todo Щеглов: добавить обработку ошибок
                for barcode in cargo_barcode['datalist']:
                    barcode_json.append({'barcode': barcode['waresbarcode']})

                wares_json['barcodes'] = barcode_json

                # цена продажи
                sql_detail_sale = """SELECT s.sale_price, s.salerestrict, s.objid, s.objname, s.objtype, s.objcode,
                                               s.objinn, s.objkpp,
                                               s.objphone, s.objemail, s.objadress, s.objadressreal, s.objfsrarid,
                                               s.objexternalid, s.objexternalcode, s.objexternaltype
                                          FROM Q_EXP_SALE_V1(?, ?) s"""

                cargo_sale = self.execute_sql(sql_detail_sale, sql_params=[wares.wares_id, 'GLOBAL'])
                sale_json = []
                # todo Щеглов: добавить обработку ошибок
                for sale in cargo_sale['datalist']:
                    sale_json.append({'sale_price': sale['sale_price'],
                                      'salerestrict': sale['salerestrict'],
                                      'shop': {'objid': sale['objid'],
                                               'objname': sale['objname'],
                                               'objtype': sale['objtype'],
                                               'objcode': sale['objcode'],
                                               'objinn': sale['objinn'],
                                               'objkpp': sale['objkpp'],
                                               'objphone': sale['objphone'],
                                               'objemail': sale['objemail'],
                                               'objadress': sale['objadress'],
                                               'objadressreal': sale['objadressreal'],
                                               'objfsrarid': sale['objfsrarid'],
                                               'objexternalid': sale['objexternalid'],
                                               'objexternalcode': sale['objexternalcode'],
                                               'objexternaltype': sale['objexternaltype']
                                               }})

                wares_json['sale_prices'] = sale_json

                # цена закупки
                sql_detail_buy = """SELECT s.bay_price, s.orderrestrict, s.objid, s.objname, s.objtype, s.objcode,
                                                   s.objinn, s.objkpp,
                                                   s.objphone, s.objemail, s.objadress, s.objadressreal, s.objfsrarid,
                                                   s.objexternalid, s.objexternalcode, s.objexternaltype,
                                                   s.cobjid, s.cobjname, s.cobjtype, s.cobjcode,
                                                   s.cobjinn, s.cobjkpp,
                                                   s.cobjphone, s.cobjemail, s.cobjadress, s.cobjadressreal, s.cobjfsrarid,
                                                   s.cobjexternalid, s.cobjexternalcode, s.cobjexternaltype
                                              FROM Q_EXP_BAY_V1(?, ?) s"""

                cargo_buy = self.execute_sql(sql_detail_buy, sql_params=[wares.wares_id, 'GLOBAL'])
                buy_json = []
                # todo Щеглов: добавить обработку ошибок
                for bay in cargo_buy['datalist']:
                    buy_json.append({'buy_price': bay['bay_price'],
                                     'orderrestrict': bay['orderrestrict'],
                                      'shop': {'objid': bay['objid'],
                                               'objname': bay['objname'],
                                               'objtype': bay['objtype'],
                                               'objcode': bay['objcode'],
                                               'objinn': bay['objinn'],
                                               'objkpp': bay['objkpp'],
                                               'objphone': bay['objphone'],
                                               'objemail': bay['objemail'],
                                               'objadress': bay['objadress'],
                                               'objadressreal': bay['objadressreal'],
                                               'objfsrarid': bay['objfsrarid'],
                                               'objexternalid': bay['objexternalid'],
                                               'objexternalcode': bay['objexternalcode'],
                                               'objexternaltype': bay['objexternaltype']
                                               },
                                     'customer': {'objid': bay['cobjid'],
                                                  'objname': bay['cobjname'],
                                                  'objtype': bay['cobjtype'],
                                                  'objcode': bay['cobjcode'],
                                                  'objinn': bay['cobjinn'],
                                                  'objkpp': bay['cobjkpp'],
                                                  'objphone': bay['cobjphone'],
                                                  'objemail': bay['cobjemail'],
                                                  'objadress': bay['cobjadress'],
                                                  'objadressreal': bay['cobjadressreal'],
                                                  'objfsrarid': bay['cobjfsrarid'],
                                                  'objexternalid': bay['cobjexternalid'],
                                                  'objexternalcode': bay['cobjexternalcode'],
                                                  'objexternaltype': bay['cobjexternaltype']
                                                  }
                                     })

                wares_json['buy_prices'] = buy_json

                result_gwares.append({'wares': wares_json})

            result_json = []
            result_json.append({'gwares': result_gwares})

            self.create_file(rule_dic, result_json)

        elif type_export == 'object':
            result_obj = []
            for obj_id in id_list:
                # obj_id = rule_dic['IDs']
                obj = M_Obj.Object.get(obj_id, self.execute_sql)
                obj_json = obj.get_json(execute_sql_func=self.execute_sql)

                result_obj.append({'object': obj_json})

            result_json = []
            result_json.append({'objects': result_obj})

            self.create_file(rule_dic, result_json)

        elif type_export == 'docrarus':
            session_id = None
            obj_id = None
            date_export = None
            try:
                session_id = rule_dic['session_id']
            except KeyError:
                pass
            try:
                obj_id = rule_dic['obj_id']
            except KeyError:
                pass
            try:
                date_export = rule_dic['date_export']
            except KeyError:
                pass

            sql_text = '''select * from Q_EXP_SALE_DOC_HEADER_LB_RARUS(?,?,?)'''
            sql_params = [session_id, obj_id, date_export]
            doc_h = self.execute_sql(sql_text,
                                     sql_params=sql_params,
                                     fetch='one')
            xml = None
            if doc_h['datalist']:
                xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + '\n'
                xml += '<purchases count="' + str(doc_h['datalist']['cnt_doc']) + '">' + '\n'

                sql_text = '''select d.*,
                                     case d.operation_type when 'true' then null else d.docid end as original_purchase
                                from Q_EXP_SALE_DOC_LB_RARUS(?,?,?) d'''
                sql_params = [session_id, obj_id, date_export]
                docs = M_doc_rarus.Docrarus.gets(self.execute_sql, sql_text, sql_params)

                for doc in docs:
                    doc.shift = session_id
                    self.create_queue_bond(doc.doc_id, 'E')
                    xml += doc.get_xml('rarus',
                                       fk_as_object=True,
                                       execute_sql_func=self.execute_sql,
                                       with_details=True,
                                       indent_level=1) + '\n'
                xml += '</purchases>'
                xml = xml.decode('cp1251').encode('utf-8')

            if xml:
                self.create_file(rule_dic, xml)
            # Z-отчет
            sql_text = 'select * from Q_EXPORT_Z_RARUS(?,?)'
            sql_params = [session_id, None]
            z_report = M_z_rarus.Zreport.get(None, self.execute_sql, sql_text=sql_text, params=sql_params)
            xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + '\n'
            xml += '<reports count="1">' + '\n'
            xml += z_report.get_xml('rarus',
                                    indent_level=1, rules_as_attr=False) + '\n'
            xml += '</reports>'
            xml = xml.decode('cp1251').encode('utf-8')
            self.create_file(rule_dic, xml, filename_key='FileNameZRarus')
        elif type_export == 'vikimini':

            # экспорт в формате vikimini
            for obj_id in id_list:
                # проверим есть ли ид оборудования
                equipmentid = None
                if 'equipmentid' in rule_dic:
                    equipmentid = rule_dic['equipmentid']

                if not self.check_viki(equipmentid):
                    continue

                # получим список кассы по магазину
                sql_text = "select * from API_GET_CASHBOX(?,?)"
                sql_params = [obj_id, None]
                if equipmentid:
                    sql_text += " where equipmentid = ?"
                    sql_params += [equipmentid]
                cash_box = self.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='many')

                # проверим может это инициализация кассы
                delete_before = False
                if 'init' in rule_dic:
                    if rule_dic['init'] == '1':
                        delete_before = True

                type_get_data = 'full'
                if 'packet' in rule_dic:
                    type_get_data = 'packet'
                for itm in cash_box['datalist']:
                    cash_desk_id = itm['EQUIPMENTID']
                    exp_path = rule_dic['ExpPath']
                    s0 = ''
                    if self.layer_code:
                        s0 = self.layer_code
                    exp_path = (exp_path % s0).replace('\\', '/')
                    exp_path = os.path.join(exp_path, 'vikimini')
                    exp_path = os.path.join(exp_path, str(cash_desk_id))

                    s1 = 'temp_'
                    now = datetime.datetime.now()
                    s2 = time.strftime('%Y%m%d%H%M%S', time.localtime()) + str(now.microsecond)

                    file_name_exp = rule_dic['FileName']
                    file_name_exp_temp = (file_name_exp % (s1, s2)).replace('\\', '/')
                    file_name_exp_temp = os.path.join(exp_path, file_name_exp_temp)
                    file_name_exp = (file_name_exp % ('', s2)).replace('\\', '/')
                    file_name_exp = os.path.join(exp_path, file_name_exp)

                    file_name_exp_temp = os.path.join(exp_path, file_name_exp_temp)
                    file_name_exp = os.path.join(exp_path, file_name_exp)
                    self.create_folder(file_name_exp_temp)
                    with vikiminiwriter(file_name_exp_temp, delete_before) as writer:
                        # Товарные группы
                        sql_text = '''select * from waresgroup'''
                        params = []
                        groups = wiki_Wg.Waresgroup.gets(self.execute_sql, sql_text, params)
                        writer.groups(groups)
                        # Товары
                        # полная выгрузка
                        if type_get_data == 'full':
                            sql_text = '''select g.*, g.is_weight_wares||',,,,,,,,,,,,,,'||g.prop_alcohol as props
                                from Q_GWARES_GET_V1(?,?,?,?) g'''
                            params = [itm['EQUIPMENTHASH'], None, None, None]
                            goods = wiki_Gw.Goods.gets(self.execute_sql, sql_text, params)
                            writer.goods(goods)
                        else:
                            # выгрузка по пакетам
                            sql_text = 'select * from API_GET_MYPACKETS(?,?)'
                            sql_params = [itm['EQUIPMENTHASH'], None]
                            res_data_packet = self.execute_sql(sql_text,
                                                               sql_params=sql_params,
                                                               fetch='many')
                            # todo проверка на ошибки
                            for data_packet in res_data_packet['datalist']:
                                sql_text = '''select g.*, g.is_weight_wares||',,,,,,,,,,,,,,'||g.prop_alcohol as props
                                    from Q_GWARES_GET_V1(?,?,?,?) g'''
                                params = [itm['EQUIPMENTHASH'], data_packet['datapacketid'], None, None]
                                goods = wiki_Gw.Goods.gets(self.execute_sql, sql_text, params)
                                writer.goods(goods)
                        # Пользователи, пароль 10 символов
                        sql_text = '''select p.uid, p.fio, substring(p.cashdeskpass from 1 for 10) as cashdeskpass
                            from my_user_cashdesk k
                            join profile p on k.uid = p.uid
                            where k.cashdeskid = ?'''
                        params = [cash_desk_id]
                        users = wiki_Us.CashUser.gets(self.execute_sql, sql_text, params)
                        writer.users(users)

                    ''' переименовываем временный файл в нормальный '''
                    # todo сообщение об ошибке
                    try:
                        self.move_file(file_name_exp_temp, file_name_exp)
                        self.log_file('Файл успешно сохранен: ' + file_name_exp, terms=1)
                        # @deprecated #1343 После завершения формирования создать файл-флаг goods_flag.txt
                        # file_flag = os.path.join(os.path.dirname(file_name_exp), 'goods_flag.txt')
                        # os.mknod(file_flag)
                        self.export_file_name = file_name_exp
                    except:
                        self.TracebackLog('Ошибка переименования файла')
                        self.log_to_db('Ошибка переименования файла')

                    if self.result['result'] == c.plugin_ok:
                        if type_get_data == 'packet':
                            for data_packet in res_data_packet['datalist']:
                                sql_text = 'execute procedure MY_KASSA_PACKET_SETRESP(?,?)'
                                sql_params = [itm['EQUIPMENTHASH'], data_packet['datapacketid']]
                                res = self.execute_sql(sql_text,
                                                       sql_params=sql_params,
                                                       fetch='none')

    def get_z_report(self, session_id):
        """
        Получение данных z-отчета из БД
        @param session_id:
        @return:
        """
        sql_text = 'select * from Q_EXPORT_Z_REPORT(?,?)'
        params = [session_id, None]
        z_report = Zreport.get(None, self.execute_sql_local, sql_text=sql_text, params=params)
        if z_report.sessionid is not None:
            return z_report
        else:
            return None

    def get_orders(self, session_id):
        """
        Получение кассовых операций
        @param session_id:
        @return:
        """
        sql_text = 'select c.*, shopid as shop, equipmentid as equipment from Q_EXPORT_CASHDESK_ORDERS(?,?) c'
        params = [session_id, None]
        return Order.gets(self.execute_sql_local, sql_text=sql_text, params=params)

    def queue_callback(self, status, filename):
        """
        Изменение статуса задания и имени файла во внешнем слое
        @param status:
        @param filename:
        @return:
        """
        sql_text = '''select * from Q_API_BACKGROUND_QUEUE_INSSEL(?,?,?,?,?,?,?,?)'''
        sql_params = [None, None, None, status,
                      None, filename, self.queueid, None]
        res = self.execute_sql_local(sql_text,
                                     sql_params=sql_params,
                                     fetch="one")
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка подтверждения результата.' + c.kr_term_enter)
            self.result['result'] = c.plugin_error
            return False
        return True

    def get_params_rule(self, key, default=None):
        """
            Получение параметров правила экспорта
        """

        try:
            result = self.export_dic[key]
        except KeyError:
            result = default
        except:
            result = default
        return result

    def execute_sql_local(self, sql_text, sql_params=(), auto_commit=True, fetch='many'):
        return self.execute_sql(sql_text,
                                sql_params=sql_params,
                                auto_commit=auto_commit,
                                db_local=self.db_local,
                                fetch=fetch)

    def create_file(self, rule_dic, result_to_save, filename_key='FileName'):
        """
        Сохранение результата в файл
        @return:
        """

        # получим имя файла если оно указано
        file_name_exp = rule_dic[filename_key]
        exp_path = rule_dic['ExpPath']
        result_format = rule_dic['result_format']
        if not file_name_exp:
            self.log_file('Неизвестен файл назначения')
            self.result['result'] = c.plugin_error
            return False

        try:
            path_sub_export = rule_dic['path_sub_export']
        except:
            path_sub_export = None
        if path_sub_export == '':
            path_sub_export = None

        # формирование имени файла
        now = datetime.datetime.now()
        if exp_path.find('%s') > -1:
            # В пути может не быть маски слоя
            s0 = ''
            if self.layer_code:
                s0 = self.layer_code
            exp_path = exp_path % s0

        s1 = 'temp_'
        s2 = time.strftime('%Y%m%d%H%M%S', time.localtime()) + str(now.microsecond)

        # https://redmine.litebox.ru/issues/3049
        # if self.sn_name:
        #     exp_path = os.path.join(exp_path, self.sn_name.replace('https://', ''))
        # else:
        #     exp_path = os.path.join(exp_path, 'UNKNOWN')
        exp_path = os.path.join(exp_path, 'out')
        if path_sub_export:
            exp_path = os.path.join(exp_path, path_sub_export)
        file_name_exp_temp = os.path.join(exp_path, file_name_exp % (s1, s2))
        file_name_exp_temp = file_name_exp_temp.replace('/',os.sep).replace('\\',os.sep)
        file_name_exp = os.path.join(exp_path, file_name_exp % ('', s2))
        file_name_exp = file_name_exp.replace('/', os.sep).replace('\\', os.sep)
        exp_path = exp_path.replace('/', os.sep).replace('\\', os.sep)

        ''' проверим существует ли каталог для экспорта файла
            проверям на существование переменной, для поддержания старых заданий
        '''

        if exp_path:
            self.create_folder(exp_path)
            # if not self.is_exists_folder(exp_path):
            #
            #     if not self.is_exists_folder(exp_path):
            #         self.log_file(c.m_e_not_exists_folder % exp_path)
            #         self.log_to_db(c.m_e_not_exists_folder % exp_path)
            #         self.result['result'] = c.plugin_error
            #         return False

        ''' преобразование кодировки файла '''
        try:
            if result_format == 'json':
                if len(result_to_save) >= 1:
                    result_to_save[0]['version'] = self.version
                json_result = decodeUStr(json.dumps(result_to_save, encoding='cp1251', indent=1).encode('cp1251'))
            #json_result = json_result.decode('cp1251').encode('utf-8')
            if result_format == 'xml':
                json_result = result_to_save
        except:
            self.TracebackLog('Ошибка перекодирования файла')
            self.log_to_db('Ошибка перекодирования файла')

        ''' сохраняем сначала во временный файл '''
        try:
            self.text_save_to_file(json_result, file_name_exp_temp)
        except:
            self.TracebackLog('Ошибка сохранения во временый файл')
            self.log_to_db('Ошибка сохранения во временый файл')

        ''' переименовываем временный файл в нормальный '''
        try:
            self.move_file(file_name_exp_temp, file_name_exp)
            self.export_file_name = file_name_exp
            return True
        except:
            self.TracebackLog('Ошибка переименования файла')
            self.log_to_db('Ошибка переименования файла')
        return False

    def check_viki(self, equipmentid):
        """
        Проверка лицензий для viki
        @param equipmentid: оборудование
        @return:
        """
        # todo Щеглов: Если не проходит проверка, писать в лог что бы понятно было
        res = self.execute_sql(sql_text='select result from Q_VIKI_CHECK(?)',
                               sql_params=[equipmentid],
                               fetch='one')

        return res['datalist']['result'] == 1
