# -*- coding: utf-8 -*-

"""
    модуль импорта данных из json используя ORM
"""

import os
import json
import operator

import krconst as c

from rbsqutils import check_number

import BasePlugin as Bp
from orm.models import Document as Doc
from orm.models import Docgood as M_Doc_goods
from orm.models import Gwares as M_Gwares
from orm.models import Object as M_Obj
from orm.models import Unit as M_Unit
from orm.models import Tax as M_Tax
from orm.models import Waresgroup as M_Wgroup

__author__ = 'swat'
VERSION = '1.0.0.2'
DATE_VERSION = '28.03.2016'


class Plugin(Bp.BasePlugin):
    """
        Класс импорта из json
    """

    str_to_file = ''

    def run(self):
        """
            Импорт данных
        """

        try:
            with open(self.filenames, 'r') as f:
                entry = json.load(f, encoding='cp1251')
        except Exception as exc:
            self.log_file(c.m_e_i_external_file % self.filenames, terms=2)
            self.log_file('Ошибка загрузки json.')
            self.log_file(exc)
            self.result['result'] = c.plugin_error
            return False

        json_data = entry[0]
        # Определяем что будет импортироваться
        if json_data.get('documents', 'error') != 'error':
            if json_data.get('documents', 'error') != 'error':
                for itm in json_data['documents']:
                    document = Doc.Document.load_from_json(itm['document'])
                    sql_text = 'select * from Q_IMP_DOC_V1(?,?,?,?,?,?,?,?,?,?,?,?,?)'
                    # получим объекты
                    document.from_obj_id = self.import_object(itm['document'], 'A', 'fromobj')
                    document.to_obj_id = self.import_object(itm['document'], 'A', 'toobj')
                    document.through_obj_id = self.import_object(itm['document'], 'A', 'throughobj')
                    document.owner_obj_id = self.import_object(itm['document'], 'A', 'ownerobj')
                    
                    dbase = itm['document'].get('dbase', None)

                    if self.result['result'] != c.plugin_error:
                        sql_params = [document.type_doc, document.number_doc, str(document.date_doc),
                                      str(document.real_date_doc), document.from_obj_id, document.to_obj_id,
                                      document.through_obj_id, document.owner_obj_id,
                                      document.external_id, document.doc_id, document.registregais,
                                      'GLOBAL', dbase]
                        doc_res = self.execute_sql(sql_text,
                                                   sql_params=sql_params,
                                                   fetch='one',
                                                   auto_commit=False)
                        if doc_res['status'] == c.kr_sql_error:
                            self.LogFile(c.m_e_i_external_file % self.filenames, Terms=2)
                            self.str_to_file = self.str_to_file + doc_res['error_db'] + c.t_enter
                            self.str_to_file = self.str_to_file + '=================================' + c.t_enter
                            self.db.rollback()
                            self.save_log_file_single()
                        else:
                            docid = doc_res['datalist']['DOCID']
                            nameprocafter = doc_res['datalist']['NAMEPROC']

                            ''' Создадим связь задания и документа '''
                            self.create_queue_bond(docid)

                            json_cargo = itm['document']['cargo']
                            for itm_cargo in json_cargo:
                                cargo = M_Doc_goods.Docgood.load_from_json(itm_cargo)

                                cargo.wares_id = self.import_gwares(cargo, itm_cargo, 'A', True)

                                sql_text = 'select * from Q_IMP_CARGO_V1(?,?,?,?,?)'
                                sql_params = [docid, cargo.wares_id,
                                              str(cargo.quantity), str(cargo.price), str(cargo.doc_sum)]
                                cargo_res = self.execute_sql(sql_text,
                                                             sql_params=sql_params,
                                                             fetch='one',
                                                             auto_commit=False)
                                if cargo_res['status'] == c.kr_sql_error:
                                    self.LogFile(c.m_e_i_external_file % self.filenames + c.t_double_enter)
                                    self.str_to_file = self.str_to_file + cargo_res['error_db'] + c.t_enter
                                    self.str_to_file = self.str_to_file + '=================================' + c.t_enter
                                else:
                                    obj_egais = None
                                    try:
                                        obj_egais = itm_cargo['egais']
                                    except KeyError:
                                        pass
                                    if not obj_egais:
                                        obj_egais = {}
                                    if len(obj_egais) > 0:
                                        for egais in obj_egais:
                                            sql_text = 'execute procedure Q_IMP_EGAIS_V1(?,?,?,?,?,?,?)'
                                            informaregid = egais['informaregid']
                                            if informaregid:
                                                informaregid = informaregid.encode('cp1251')
                                            informbregid = egais['informbregid']
                                            if informbregid:
                                                informbregid = informbregid.encode('cp1251')
                                            ttninformbregid = egais['ttninformbregid']
                                            if ttninformbregid:
                                                ttninformbregid = ttninformbregid.encode('cp1251')
                                            try:
                                                identity = str(egais['identity'])
                                            except:
                                                identity = None
                                            if identity:
                                                identity = identity.encode('cp1251')

                                            sql_params = [docid, cargo_res['datalist']['imp_positionid'],
                                                          informbregid, informaregid,
                                                          ttninformbregid, egais['amount'], identity]
                                            cargo_egais = self.execute_sql(sql_text,
                                                                           sql_params=sql_params,
                                                                           fetch='none',
                                                                           auto_commit=False)
                                            if cargo_egais['status'] == c.kr_sql_error:
                                                self.LogFile(c.m_e_i_external_file % self.filenames + c.t_double_enter)
                                                self.str_to_file = self.str_to_file + cargo_egais['error_db'] + c.t_enter
                                                self.str_to_file = self.str_to_file + '=================================' + c.t_enter

                        if self.result['result'] == c.plugin_error:
                            self.save_log_file_single()
                            self.db.rollback()
                            return False
                        if len(nameprocafter) == 1:
                            upstatus = self.execute_sql('execute procedure Q_IMP_UPSTATUS_V1(?,?)',
                                                        sql_params=[docid, '1'],
                                                        fetch='one',
                                                        auto_commit=False)
                            if upstatus['status'] == c.kr_sql_error:
                                self.LogFile(c.m_e_i_external_file % self.filenames + c.t_double_enter)
                                self.str_to_file = self.str_to_file + upstatus['error_db'] + c.t_enter
                                self.str_to_file = self.str_to_file + '=================================' + c.t_enter
                        else:
                            procafterimport = self.execute_sql('execute procedure ' + nameprocafter + '(?)',
                                                               sql_params=[docid],
                                                               fetch='one',
                                                               auto_commit=False)
                            if procafterimport['status'] == c.kr_sql_error:
                                self.LogFile(c.m_e_i_external_file % self.filenames + c.t_double_enter)
                                self.str_to_file = self.str_to_file + procafterimport['error_db'] + c.t_enter
                                self.str_to_file = self.str_to_file + '=================================' + c.t_enter

                        if self.result['result'] == c.plugin_ok:
                            self.db.commit()
                        else:
                            self.db.rollback()
                            self.save_log_file_single()

        if json_data.get('gwares', 'error') != 'error':
            # если есть поле wares_parent, то сделаем сортировку по нему
            json_gwares = json_data['gwares']
            if 'wares_parent' in json_data['gwares']:
                try:
                    json_gwares = sorted(json_gwares, key=operator.itemgetter("wares_parent"), reverse=True)
                except:
                    self.log_file('Ошибка сортировки по ключу wares_parent', terms=1)

            for itm in json_gwares:
                wares = M_Gwares.Gwares.load_from_json(itm['wares'])
                if 'min_sale_prices' in itm['wares'] and itm['wares']['min_sale_prices'] is not None:
                    # Товар содержит только минимальную цену продажи
                    wares_id = self.import_gwares(wares, itm['wares'], 'S', False)
                else:
                    wares_id = self.import_gwares(wares, itm['wares'], 'I', False)

            sql_text = 'execute procedure IMP_PRICE_PROPAGATE'
            propagate = self.execute_sql(sql_text,
                                         sql_params=[],
                                         fetch='none')
            if propagate['status'] == c.kr_sql_error:
                self.LogFile(c.m_e_i_external_file % self.filenames + c.t_double_enter)
                self.str_to_file += 'Ошибка распространения цен для всех магазинов'
                self.str_to_file = self.str_to_file + '=================================' + c.t_enter

            # if self.result['result'] == c.plugin_ok:
            sql_text = 'execute procedure IMP_PRICE_SYNC'
            sync_price = self.execute_sql(sql_text,
                                          sql_params=[],
                                          fetch='one',
                                          auto_commit=True)
            if sync_price['status'] == c.kr_sql_error:
                self.LogFile(c.m_e_i_external_file % self.filenames + c.t_double_enter)
                self.str_to_file += 'Ошибка синхронизации цен'
                self.str_to_file = self.str_to_file + '=================================' + c.t_enter

            self.save_log_file_single()

        if json_data.get('objects', 'error') != 'error':
            for itm in json_data['objects']:
                self.import_object(itm['object'], 'I')

            self.save_log_file_single()

    def save_log_file_single(self):
        """
            Сохранение файла лога в директорию слоя
        """
        if self.str_to_file == '':
            self.str_to_file = 'OK'

        log_file = os.path.basename(self.filenames)
        log_file = log_file.replace('.json', '.log')
        dir_file = '/root/Dropbox/' + self.sn_name.replace('https://', '') + self.layer_code + '/out/'
        log_file_tmp = os.path.join(dir_file, 'tmp_' + log_file)
        log_file = os.path.join(dir_file, log_file)

        ''' сохраняем сначала во временный файл '''
        try:
            self.text_save_to_file(self.str_to_file, log_file_tmp)
        except:
            self.TracebackLog('Ошибка сохранения во временый файл')
            self.log_to_db('Ошибка сохранения во временый файл')

        ''' переименовываем временный файл в нормальный '''
        try:
            self.move_file(log_file_tmp, log_file)
        except:
            self.TracebackLog('Ошибка переименования файла')
            self.log_to_db('Ошибка переименования файла')

    def import_object(self, json_data, flag, prefix='', type_load='json', category=None):
        """
        импорт объектов
        @param json_data: данные
        @param flag: флаг импорта
        @param prefix: префикс если данные идут скопом
        @param type_load: тип загрузки: json сразу через модель
        @param category: категория объекта
        @return: id объекта
        """

        obj_id = None
        if type_load == 'json':
            obj = M_Obj.Object.load_from_json(json_data, prefix)
            if not obj.fsrar_id:
                try:
                    obj.fsrar_id = json_data['fsrarid']
                except KeyError:
                    pass
            if not obj.fsrar_id:
                try:
                    obj.fsrar_id = json_data[prefix + 'fsrarid']
                except KeyError:
                    pass
            if not obj.address:
                try:
                    obj.address = json_data[prefix + 'adress']
                except KeyError:
                    pass
            if not obj.address_real:
                try:
                    obj.address_real = json_data[prefix + 'adressreal']
                except KeyError:
                    pass
            if not obj.obj_id:
                try:
                    obj.obj_id = json_data[prefix + 'id']
                except KeyError:
                    pass
        else:
            obj = M_Obj.Object()
            obj.obj_id = json_data['objid']
            obj.obj_type = json_data['objtype']
            obj.code = json_data['objcode']
            obj.name = json_data['objname']
            obj.kpp = json_data['objkpp']
            obj.inn = json_data['objinn']
            obj.fsrar_id = json_data['objfsrarid']
            obj.phone = json_data['objphone']
            obj.email = json_data['objemail']
            obj.address = json_data['objadress']
            if not obj.address:
                try:
                    obj.address = json_data['objaddress']
                except:
                    pass
            obj.address_real = json_data['objadressreal']
            if not obj.address_real:
                try:
                    obj.address_real = json_data['objaddressreal']
                except:
                    pass
            obj.external_id = json_data['objexternalid']
            obj.external_code = json_data['objexternalcode']
            obj.external_type = json_data['objexternaltype']
        if not obj.external_id and not obj.external_code and not obj.obj_id and not obj.code:
            return None

        params = {'exchangetaskcode': 'GLOBAL',
                  'flag': flag,
                  'category': category}

        if obj.save(execute_sql_func=self.execute_sql, params=params):
            obj_id = obj.obj_id
        else:
            self.log_file(c.m_e_i_external_file % self.filenames, terms=2)
            if not obj.name:
                msg = obj.external_code
            else:
                msg = obj.name
            if flag == 'I':
                self.str_to_file += 'Ошибка импорта объекта:' + msg + c.t_enter
            if flag == 'A':
                self.str_to_file += 'Ошибка поиска объекта:' + msg + c.t_enter

        return obj_id

    def import_tax(self, orm_model, flag):
        """
        Импорт налоговых
        @param orm_model: объект
        @param flag: флаг
        @return: id
        """

        tax_id = None

        tax = M_Tax.Tax()
        tax.tax_id = orm_model.tax_id
        tax.name = orm_model.tax_name
        tax.short_name = orm_model.tax
        tax.rate = float(orm_model.tax_rate)
        tax.external_id = orm_model.tax_external_id
        tax.external_code = orm_model.tax_external_code
        params = {'exchangetaskcode': 'GLOBAL',
                  'flag': flag}

        if tax.save(execute_sql_func=self.execute_sql, params=params):
            tax_id = tax.tax_id
        else:
            self.log_file(c.m_e_i_external_file % self.filenames, terms=2)
            self.str_to_file += 'Ошибка сохранения налога с кодом:' + tax.name + c.t_enter

        return tax_id

    def import_units(self, orm_model, flag):
        """
        Импорт налоговых
        @param orm_model: объект
        @param flag: флаг
        @return: id
        """

        # модель ед измерения и товара, позиции документа вышла не согласованная, поэтому такая дибильная загрузка
        unit_id = None

        unit = M_Unit.Unit()
        unit.unit_id = orm_model.main_id
        # проверки если short_name числовое значение, то берем name
        if check_number(orm_model.main_unit):
            unit.short_name = orm_model.main_unit_name
        else:
            unit.short_name = orm_model.main_unit
        unit.name = orm_model.main_unit_name
        if orm_model.main_unit_factor:
            unit.factor = str(orm_model.main_unit_factor)
        else:
            unit.factor = None
        unit.external_id = orm_model.main_unit_external_id
        unit.external_code = orm_model.main_unit_external_code
        if unit.name or unit.short_name or unit.external_id or unit.external_code:
            params = {'exchangetaskcode': 'GLOBAL',
                      'flag': flag}
            if unit.save(execute_sql_func=self.execute_sql, params=params):
                unit_id = unit.unit_id
            else:
                self.log_file(c.m_e_i_external_file % self.filenames, terms=2)
                try:
                    self.str_to_file += 'Ошибка сохранения ед измерения с кодом:' + unit.name + c.t_enter
                except:
                    self.str_to_file += 'Ошибка сохранения ед измерения.' + c.t_enter
        else:
            self.log_file(c.m_e_i_external_file % self.filenames, terms=2)
            self.str_to_file += 'Ошибка: нет данных по ед измерения ' + c.t_enter
        return unit_id

    def import_wgroup_parent(self, json_data, flag):
        """
        Импорт групп по иерархии
        @param json_data: данные
        @param flag: флаг
        @return: id
        """

        wgroup_id = None
        json_data = json_data['parent_wgroup']
        if json_data:
            json_data = sorted(json_data, key=operator.itemgetter("wglevelnumber"))

            wgroup_id = None

            for itm in json_data:
                wgroup_id = self.import_wgroup(itm, flag, wgroup_id)
        return wgroup_id

    def import_wgroup(self, json_data, flag, higher_id):
        """
        Импорт групп
        @param json_data: данные
        @param flag: флаг
        @param higher_id: ид родителя
        @return: id
        """

        wgroup_id = None

        wgroup = M_Wgroup.Waresgroup.load_from_json(json_data)
        wgroup.higher = higher_id

        params = {'exchangetaskcode': 'GLOBAL',
                  'flag': flag,
                  'deletemarker': '0'}

        if wgroup.save(execute_sql_func=self.execute_sql, params=params):
            wgroup_id = wgroup.wares_group_id
        else:
            self.log_file(c.m_e_i_external_file % self.filenames, terms=2)
            if flag == 'I':
                self.str_to_file += 'Ошибка импорта товарной группы:' + wgroup.name + c.t_enter
            if flag == 'A':
                self.str_to_file += 'Ошибка поиска товарной группы:' + wgroup.name + c.t_enter

        return wgroup_id

    def import_gwares(self, orm_model, json_data, flag, need_obj_prefix):
        """
        Импорт групп
        @param orm_model: объект
        @param json_data: json
        @param flag: флаг
        @param need_obj_prefix: нужно ли передавать префикс при импорте объектов
        @return: id
        """

        wares_id = None

        cnt_error = 0
        if flag == 'I':
            # получим основную ед измерения
            unit_id = self.import_units(orm_model, flag)
            if unit_id is None:
                cnt_error += 1

            tax_id = self.import_tax(orm_model, flag)
            if tax_id is None:
                cnt_error += 1

            importer_id = None
            if json_data['importer'] is not None:
                if len(json_data['importer']) > 0:
                    prefix = ''
                    if need_obj_prefix:
                        prefix = 'importer'
                    importer_id = self.import_object(json_data['importer'], flag, prefix=prefix, category='IMPORTER')
                    if importer_id is None:
                        cnt_error += 1

            producer_id = None
            if json_data['producer'] is not None:
                if len(json_data['producer']) > 0:
                    prefix = ''
                    if need_obj_prefix:
                        prefix = 'producer'
                    producer_id = self.import_object(json_data['producer'], flag, prefix=prefix, category='PRODUCER')
                    if producer_id is None:
                        cnt_error += 1

            higher_id = self.import_wgroup_parent(json_data, flag)
            wgroup_id = self.import_wgroup(json_data, flag, higher_id)
        else:
            unit_id = None
            tax_id = None
            wgroup_id = None

        if cnt_error == 0:
            sql_text = 'select * from Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?)'
            sql_params = [orm_model.wares_external_id, orm_model.wares_external_code,
                          orm_model.wares_code, orm_model.wares_name, unit_id, tax_id, wgroup_id,
                          flag, '0',
                          orm_model.wares_id, 'GLOBAL']

            wares_res = self.execute_sql(sql_text,
                                         sql_params=sql_params,
                                         fetch='one')

            if wares_res['status'] == c.kr_sql_error:
                self.LogFile(c.m_e_i_external_file % self.filenames, Terms=2)
                self.str_to_file += wares_res['error_db'] + c.t_enter
                cnt_error += 1

            else:
                wares_id = wares_res['datalist']['waresid']

        if wares_id and flag != 'A':
            try:
                if len(json_data['barcodes']):
                    if not self.import_barcode(wares_id, unit_id, json_data['barcodes']):
                        cnt_error += 1
            except:
                pass
            try:
                if not self.import_wares_price(wares_id, json_data):
                    cnt_error += 1
            except:
                pass
            try:
                if not self.import_wares_price(wares_id, json_data, type_price='MIN_SALE'):
                    cnt_error += 1
            except:
                pass
            try:
                if not self.import_property(wares_id, importer_id, producer_id, json_data):
                    cnt_error += 1
            except:
                pass
        if cnt_error != 0:
            if orm_model.wares_name:
                self.str_to_file += 'Ошибка импорта товара:' + orm_model.wares_name + c.t_enter
            else:
                self.str_to_file += 'Ошибка импорта товара:' + orm_model.wares_code + c.t_enter
            self.str_to_file += '=================================' + c.t_enter
            self.result['result'] = c.plugin_error
        return wares_id

    def import_barcode(self, waresid, unitid, json_data):
        """
        Импорт ШК товара
        @param waresid: ид товара
        @param unitid: ид ед измерения товара
        @param json_data: json
        @return:
        """

        result = True
        str_barcode = ''
        for itm in json_data:
            str_barcode += ' ' + itm['barcode'].encode('cp1251')
        if str_barcode != '':
            sql_text = 'select * from Q_WARESBARCODE_INSEL(?,?,?)'
            sql_params = [waresid, unitid, str_barcode]
            wares_res = self.execute_sql(sql_text,
                                         sql_params=sql_params,
                                         fetch='one')

            if wares_res['status'] == c.kr_sql_error:
                self.LogFile(c.m_e_i_external_file % self.filenames, Terms=2)
                self.str_to_file += 'Ошибка импорта ШК'
                result = False
        return result

    def import_price(self, obj_id, wares_id, data, type_price):
        sale_price = float(data['sale_price'])
        try:
            begin_date = str(data['begin_date'].encode('cp1251')).replace('T', ' ')
        except:
            begin_date = None
        try:
            end_date = str(data['end_date'].encode('cp1251')).replace('T', ' ')
        except:
            end_date = None
        week_day = begin_time = end_time = None
        if type_price == 'MIN_SALE':
            try:
                week_day_tmp = data['week_day']
                week_day_tmp = [x.encode('cp1251') for x in week_day_tmp]
                week_day_tmp_= sorted(week_day_tmp)
                cnt_day = 0
                week_day = ''
                while cnt_day < 7:
                    try:
                        if week_day_tmp[cnt_day].encode('cp1251') == str(cnt_day + 1):
                            week_day += '1'
                        else:
                            week_day += '0'
                    except:
                        week_day += '0'
                    cnt_day += 1
            except:
                week_day = None
            try:
                begin_time = str(data['begin_time'].encode('cp1251'))
            except:
                begin_time = None
            try:
                end_time = str(data['end_time'].encode('cp1251'))
            except:
                end_time = None

        try:
            is_delete = str(data['is_delete']).encode('cp1251')
            if is_delete == 'None':
                is_delete = 0
        except:
            is_delete = 0

        try:
            salerestrict = str(data['salerestrict']).encode('cp1251')
            if salerestrict == 'None':
                salerestrict = None
        except:
            salerestrict = None
        sql_text = 'execute procedure Q_IMP_PRICE_INS(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [obj_id, wares_id, begin_date, end_date, sale_price, None,
                      salerestrict, type_price, begin_time, end_time, str(is_delete), week_day, None, None]
        sale_res = self.execute_sql(sql_text,
                                    sql_params=sql_params,
                                    fetch='none')

        if sale_res['status'] == c.kr_sql_error:
            self.LogFile(c.m_e_i_external_file % self.filenames, Terms=2)
            self.str_to_file += 'Ошибка импорта цены ' + type_price
            return False
        return True

    def import_wares_price(self, wares_id, json_data, type_price='SALE'):
        """
        Импорт цен реализации
        @param wares_id: товар
        @param json_data: данные
        @param type_price: Тип импорта
        @return: признак импорта
        """
        result = True
        if type_price == 'MIN_SALE':
            key = 'min_sale_prices'
        else:
            key = 'sale_prices'
        shops_len = len(json_data[key])
        if shops_len == 1 and json_data[key][0]['shop'] is None:
            # Если одна запись и магазин не указан, то это цена товара для всех магазинов
            if not self.import_price(None, wares_id, json_data[key][0], type_price):
                return False
        else:
            for itm in json_data[key]:
                # Применяем к определенному магазину
                objid = self.import_object(itm['shop'], flag='I', type_load='hand')
                if objid:
                    if not self.import_price(objid, wares_id, itm, type_price):
                        result = False
                else:
                    result = False
        return result

    def import_property(self, wares_id, importer_id, producer_id, json_data):
        """
        Импорт цен реализации
        @param wares_id: товар
        @param importer_id:
        @param producer_id:
        @param json_data: данные
        @return: признак импорта
        """
        result = True

        country_code = json_data['country_code']
        if country_code:
            country_code = country_code.encode('cp1251')
        country_name = json_data['country_name']
        if country_name:
            country_name = country_name.encode('cp1251')
        wareskindcode = json_data['wareskindcode']
        if wareskindcode:
            wareskindcode = wareskindcode.encode('cp1251')
        wareskindname = json_data['wareskindname']
        if wareskindname:
            wareskindname = wareskindname.encode('cp1251')
        alccode = json_data['alccode']
        if alccode:
            alccode = alccode.encode('cp1251')
        volumevalue = json_data['volumevalue']
        if volumevalue:
            volumevalue = float(volumevalue)
        if volumevalue == '':
            volumevalue = None
        proofvalue = json_data['proofvalue']
        if proofvalue:
            proofvalue = float(proofvalue)
        if proofvalue == '':
            proofvalue = None
        try:
            wares_parent = json_data['wares_parent']
        except:
            wares_parent = None

        try:
            excise = json_data['excise']
            if excise:
                excise = excise.encode('cp1251')
        except:
            excise = None

        sql_text = 'select * from Q_GWARES_PROPERTY(?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [wares_id, importer_id, producer_id, proofvalue, volumevalue, alccode,
                      country_code, country_name, wareskindcode, wareskindname, wares_parent,
                      excise]
        property_w = self.execute_sql(sql_text,
                                      sql_params=sql_params,
                                      fetch='one')

        if property_w['status'] == c.kr_sql_error:
            self.LogFile(c.m_e_i_external_file % self.filenames, Terms=2)
            self.str_to_file += 'Ошибка импорта свойств товара.'
            result = False
        else:
            if property_w['datalist']['ERROR_CODE'] == 1:
                self.LogFile(c.m_e_i_external_file % self.filenames, Terms=2)
                self.LogFile('Данный %s алкокод есть у друго товара' % alccode, Terms=2)
                self.str_to_file += 'Ошибка импорта свойств товара.'
                self.str_to_file += 'Данный %s алкокод есть у друго товара' % alccode
                result = False
        return result
