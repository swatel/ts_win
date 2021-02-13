# -*- coding: utf-8 -*-

"""
    Модуль конвертации xml формата Rarus в json стандарт LiteBox
"""

import os

import json
import time
import datetime

import krconst as c
import BasePlugin as Bp
from rbsqutils import decodeUStr


class Plugin(Bp.BasePlugin):
    week_days = {
        'MO': '1',
        'TU': '2',
        'WE': '3',
        'TH': '4',
        'FR': '5',
        'SA': '6',
        'SU': '7'
    }

    def run(self):
        """
        Конвертация файла
        @return: Bool
        """

        file_name_dest = self.parser_xml(self.queueparamsxml, 'FileNameDest').replace('\\', '/')

        xml_file = self.parse_file_xml(self.filenames)
        if self.result['result'] == c.plugin_error:
            return False

        # проверим что находиться в файле
        xml_root = xml_file.getroot()
        if xml_root.tag == 'goods-catalog':
            self.process_goods(xml_root, file_name_dest)
        elif xml_root.tag == 'purchases':
            self.process_purchases(xml_root, file_name_dest)
        else:
            # данная ошибка не является ошибкой выполнения плагина
            message = c.m_e_i_not_need_data_in_file % self.filenames
            self.log_file(message,
                          terms=2,
                          save_log_db=True)
            return False

    def process_goods(self, xml_root, file_name_dest):
        json_wares = []
        # справочник товаров
        gwares = xml_root.findall('good')
        if gwares is not None:
            json_wares.extend(self.convert_gwares(gwares))
        min_sale_prices = xml_root.findall('min-price-restriction')
        if min_sale_prices is not None:
            json_wares.extend(self.convert_min_sale_prices(min_sale_prices))

        if len(json_wares) > 0:
            result_gwares = list()
            result_gwares.append({"gwares": json_wares})
            json_result = decodeUStr(json.dumps(result_gwares, encoding='cp1251', indent=1).encode('cp1251'))
            self.create_file(json_result, file_name_dest, '%sgwares_%s.json')

        sale_denied = xml_root.findall('sale-denied-restriction')
        if sale_denied is not None:
            pass

        max_discount = xml_root.findall('max-discount-restriction')
        if max_discount is not None:
            pass

    def process_purchases(self, xml_root, file_name_dest):
        json_purchases = []
        # продажи
        purchases = xml_root.findall('purchase')
        if purchases is not None:
            json_purchases.extend(self.convert_purchases(purchases))
        if len(json_purchases) > 0:
            result = list()
            result.append({"sales": json_purchases})
            json_result = decodeUStr(json.dumps(result, encoding='cp1251', indent=1).encode('cp1251'))
            self.create_file(json_result, file_name_dest, '%ssales_%s.json')

    def create_file(self, text, filename, file_mask):
        file_name_exp = os.path.join(filename, file_mask)
        now = datetime.datetime.now()
        s2 = time.strftime('%Y%m%d%H%M%S', time.localtime()) + str(now.microsecond)
        try:
            file_name_exp = (file_name_exp % ('', s2)).replace('\\', '/')
        except:
            pass
        try:
            self.text_save_to_file(text, file_name_exp)
        except:
            self.TracebackLog('Ошибка сохранения во временый файл')
            self.log_to_db('Ошибка сохранения во временый файл')

    def convert_purchases(self, elements):
        result_json = []
        for element in elements:
            externalid = None
            namedoctype = None
            status = None
            docid = None
            doc_sum = float(self.xml_attr_value(element, 'amount'))
            username = self.xml_attr_value(element, 'username')
            # operationType: true – продажа, false - возврат
            doctcode = 'SALE' if self.xml_attr_value(element, 'operationType') else 'RET'
            numberdoc = self.xml_attr_value(element, 'number')
            docdate = self.xml_attr_value(element, 'saleTime')
            shop = {
                "externalcode": self.xml_attr_value(element, 'shop'),
                "objid": None,
                "code": None,
                "name": None,
                "objtype": "C",
                "inn": None,
                "kpp": None,
                "fsrarid": None,
                "email": None,
                "phone": None,
                "address": None,
                "addressreal": None,
                "externalid": None,
                "externaltype": "D",
            }

            equipment = {
                'equipmentid': None,
                'equiptypeid': None,
                'status': None,
                'name': None,
                'regnum': None,
                'serialnum': None,
                'equipmenthash': None,
                'externalcode': self.xml_attr_value(element, 'cash'),
                'externalid': None
            }

            positions = []
            for position in element.find('positions').findall('position'):
                positions.append({
                    'waresexternalid': None,
                    'waresexternalcode': self.xml_attr_value(position, 'goodsCode'),
                    'warescode': None,
                    'waresname': None,
                    'price': float(self.xml_attr_value(position, 'cost')),
                    'quantity': float(self.xml_attr_value(position, 'count')),
                    'docsum': float(self.xml_attr_value(position, 'amount')),
                    'taxid': None,
                    'taxname': None,
                    'tax': None,
                    'taxrate': None,
                    'taxexternalid': None,
                    'taxexternalcode': None,
                    'selfcalc': None,
                    'mainunitid': None,
                    'mainunit': None,
                    'mainunitname': None,
                    'mainunitfactor': None,
                    'mainunitexternalid': None,
                    'mainunitexternalcode': None,
                    'barcodes': [self.xml_attr_value(position, 'barCode')]
                })

            prepared_payments = []
            change = 0.0
            for payment in element.find('payments').findall('payment'):
                # CashPaymentEntity – наличные
                # CashChangePaymentEntity – сдача
                # BankCardPaymentEntity – безналичный
                # GiftCardPaymentEntity – подарочная карта
                # ConsumerCreditPaymentEntity – потребительский кредит
                # ExternalBankTerminalPaymentEntity - внешний банковский терминал
                # BonusCardPaymentEntity - оплата бонусами
                # ChildrenCardPaymentEntity - оплата Детской картой
                type_class = self.xml_attr_value(payment, 'typeClass')
                if type_class in ['CashPaymentEntity',
                                  'BankCardPaymentEntity',
                                  'ExternalBankTerminalPaymentEntity']:
                    prepared_payments.append({
                        'type': 'CASH' if type_class == 'CashPaymentEntity' else 'NONCASH',
                        'sum': float(self.xml_attr_value(payment, 'amount'))
                    })
                elif type_class == 'CashChangePaymentEntity':
                    change += float(self.xml_attr_value(payment, 'amount'))

            payments = list()
            for payment in prepared_payments:
                if change > 0.0 and payment['type'] == 'CASH':
                    payment_sum = payment['sum'] - change
                    change = 0.0
                else:
                    payment_sum = payment['sum']
                payments.append({
                    'type': payment['type'],
                    'sum': payment_sum
                })

            element_json = {
                'externalid': externalid,
                'namedoctype': namedoctype,
                'sum': doc_sum,
                'status': status,
                'docid': docid,
                'username': username,
                'doctcode': doctcode,
                'numberdoc': numberdoc,
                'docdate': docdate,
                'shop': shop,
                'equipment': equipment,
                'positions': positions,
                'payments': payments
            }
            result_json.append(element_json)
        return result_json

    def convert_gwares(self, gwares):
        """
            Импорт товаров
        """

        gwares_json = []

        for obj in gwares:
            # Конвертируем товарные группы по иерархии
            wgroup = obj.find('group')
            wgroup_parent_json = []
            waresgrid = None
            wgcode = None
            wgname = None
            wglevelnumber = None
            wgexternalid = None
            wgexternalcode = None
            if wgroup is not None:
                # получим сначала основную товарную группу
                waresgrid = None
                wgcode = None
                wgname = self.xml_value(wgroup, 'name')
                wglevelnumber = None
                wgexternalid = self.xml_attr_value(wgroup, 'id')
                wgexternalcode = None

                wgroup_parent = wgroup.find('parent-group')
                wgroup_parent_json = self.convert_wgroup(wgroup_parent)
                wglevelnumber = len(wgroup_parent_json) + 1

            # ед измерения
            units = obj.find('measure-type')
            unit_json = self.convert_unit(units)

            # товары
            taxid = None
            taxname = self.xml_value(obj, 'vat')
            tax = self.xml_value(obj, 'vat')
            taxrate = self.xml_value(obj, 'vat')
            taxexternalid = self.xml_value(obj, 'vat')
            taxexternalcode = None

            country_code = None
            country_name = None

            mainunitid = unit_json['mainunitid']
            mainunit = unit_json['mainunit']
            mainunitname = unit_json['mainunitname']
            mainunitfactor = unit_json['mainunitfactor']
            mainunitexternalid = unit_json['mainunitexternalid']
            mainunitexternalcode = unit_json['mainunitexternalcode']

            importer = []
            producer = []

            warescode = self.xml_attr_value(obj, 'marking-of-the-good')
            waresname = self.xml_value(obj, 'name')
            alccode = None
            waresexternalid = self.xml_attr_value(obj, 'marking-of-the-good')
            waresexternalcode = None

            parent_wgroup = wgroup_parent_json

            property_json = self.convert_property(obj)

            proofvalue = property_json['proofvalue']
            volumevalue = property_json['volumevalue']
            wareskindcode = None
            wareskindname = None
            try:
                excise = self.xml_value(obj, 'excise')
                if excise == 'true':
                    excise = '1'
                elif excise == 'false':
                    excise = '0'
                else:
                    excise = None
            except:
                excise = None
            buy_prices = []
            sale_prices = self.convert_price(obj)
            barcodes = self.convert_barcode(obj)

            wares_json = {"volumevalue": volumevalue,
                          "taxid": taxid,
                          "taxname": taxname,
                          "tax": tax,
                          "taxrate": taxrate,
                          "taxexternalid": taxexternalid,
                          "taxexternalcode": taxexternalcode,
                          "wgcode": wgcode,
                          "country_code": country_code,
                          "proofvalue": proofvalue,
                          "wgexternalid": wgexternalid,
                          "wgexternalcode": wgexternalcode,
                          "waresname": waresname,
                          "mainunitid": mainunitid,
                          "mainunit": mainunit,
                          "mainunitname": mainunitname,
                          "mainunitfactor": mainunitfactor,
                          "mainunitexternalid": mainunitexternalid,
                          "mainunitexternalcode": mainunitexternalcode,
                          "importer": importer,
                          "producer": producer,
                          "alccode": alccode,
                          "waresexternalid": waresexternalid,
                          "waresexternalcode": waresexternalcode,
                          "country_name": country_name,
                          "wglevelnumber": wglevelnumber,
                          "waresgrid": waresgrid,
                          "wgname": wgname,
                          "wareskindcode": wareskindcode,
                          "warescode": warescode,
                          "excise": excise,
                          "wareskindname": wareskindname,
                          "parent_wgroup": parent_wgroup,
                          "buy_prices": buy_prices,
                          "sale_prices": sale_prices,
                          "barcodes": barcodes}
            gwares_json.append({"wares": wares_json})
        return gwares_json

    def convert_wgroup(self, wgroup):
        """
        Конвертауия товарных групп
        @param wgroup: xml
        @return: json
        """

        result_json = []
        if wgroup is not None:
            wgroup_parent = wgroup.find('parent-group')
            higher_json = self.convert_wgroup(wgroup_parent)

            if len(higher_json):
                for itm in higher_json:
                    result_json.append(itm)
                waresgrid = None
                wgcode = None
                wgname = self.xml_value(wgroup, 'name')
                wglevelnumber = len(result_json) + 1
                wgexternalid = self.xml_attr_value(wgroup, 'id')
                wgexternalcode = None

                result_json.append({"waresgrid": waresgrid,
                                    "wgname": wgname,
                                    "wglevelnumber": wglevelnumber,
                                    "wgexternalid": wgexternalid,
                                    "wgexternalcode": wgexternalcode,
                                    "wgcode": wgcode})
            else:
                waresgrid = None
                wgcode = None
                wgname = self.xml_value(wgroup, 'name')
                wglevelnumber = 1
                wgexternalid = self.xml_attr_value(wgroup, 'id')
                wgexternalcode = None

                result_json.append({"waresgrid": waresgrid,
                                    "wgname": wgname,
                                    "wglevelnumber": wglevelnumber,
                                    "wgexternalid": wgexternalid,
                                    "wgexternalcode": wgexternalcode,
                                    "wgcode": wgcode})

        return result_json

    def convert_unit(self, obj):
        """
            Импорт ед измерения
        """

        main_unit_name = self.xml_value(obj, 'name')
        main_unit_externalid = self.xml_attr_value(obj, 'id')

        result_json = {"mainunitid": None,
                       "mainunit": None,
                       "mainunitname": main_unit_name,
                       "mainunitfactor": None,
                       "mainunitexternalid": main_unit_externalid,
                       "mainunitexternalcode": None}
        return result_json

    def convert_barcode(self, obj):
        """
        Конвертация ШК
        @param obj: xml
        @return: json
        """
        barcode_json = []
        for itm in obj.findall('bar-code'):
            barcode_json.append({'barcode': self.xml_attr_value(itm, 'code')})
        return barcode_json

    def convert_price(self, obj):
        """
        Конвертаци цены продажи
        @param obj: xml
        @return: json
        """

        sale_json = []

        shops_str = self.xml_value(obj, 'shop-indices')
        if shops_str is None or shops_str == '':
            # Для всех магазинов
            obj_price = obj.find('price-entry')
            price = self.xml_attr_value(obj_price, 'price')
            begin_date = self.xml_value(obj_price, 'begin-date')
            end_date = self.xml_value(obj_price, 'end-date')
            is_delete = self.xml_value(obj, 'delete-from-cash')
            sale_json.append({"shop": None,
                              "sale_price": price,
                              "salerestrict": '0' if is_delete == 'false' else '1',
                              "begin_date": begin_date,
                              "end_date": end_date
                              })
        else:
            # Каждый магазин отдельно
            try:
                shops_str.split(',')
            except:
                return sale_json
            for itm in shops_str.split(','):
                shop_json = {"objexternalcode": itm,
                             "objemail": None,
                             "objinn": None,
                             "objkpp": None,
                             "objcode": None,
                             "objadress": None,
                             "objphone": None,
                             "objfsrarid": None,
                             "objname": None,
                             "objexternalid": None,
                             "objexternaltype": "D",
                             "objadressreal": None,
                             "objid": None,
                             "objtype": "C"
                             }
                obj_price = obj.find('price-entry')
                price = self.xml_attr_value(obj_price, 'price')
                begin_date = self.xml_value(obj_price, 'begin-date')
                end_date = self.xml_value(obj_price, 'end-date')
                is_delete = self.xml_value(obj, 'delete-from-cash')
                if is_delete == 'false':
                    is_delete = '0'
                else:
                    is_delete = '1'
                sale_json.append({"shop": shop_json,
                                  "sale_price": price,
                                  "salerestrict": is_delete,
                                  "begin_date": begin_date,
                                  "end_date": end_date
                                  })
        return sale_json

    def convert_property(self, obj):
        """
        Конвертация свойств товара
        @param obj: xml
        @return: json
        """

        proof_value = None
        volume_value = None
        # type_wares = self.xml_value(obj, 'product-type')

        for itm in obj.findall('plugin-property'):
            key = self.xml_attr_value(itm, 'key')
            if key == 'alcoholic-content-percentage':
                proof_value = self.xml_attr_value(itm, 'value')
            if key == 'volume':
                volume_value = self.xml_attr_value(itm, 'value')

        return {'volumevalue': volume_value,
                'proofvalue': proof_value}

    def convert_min_sale_prices(self, min_sale_prices):
        """
            Импорт минимальных цен продажи
        """
        gwares_json = []

        for obj in min_sale_prices:
            warescode = self.xml_attr_value(obj, 'subject-code')
            waresexternalid = warescode
            min_sale_prices = self.convert_min_prices(obj)

            wares_json = {"volumevalue": None,
                          "taxid": None,
                          "taxname": None,
                          "tax": None,
                          "taxrate": None,
                          "taxexternalid": None,
                          "taxexternalcode": None,
                          "wgcode": None,
                          "country_code": None,
                          "proofvalue": None,
                          "wgexternalid": None,
                          "wgexternalcode": None,
                          "waresname": None,
                          "mainunitid": None,
                          "mainunit": None,
                          "mainunitname": None,
                          "mainunitfactor": None,
                          "mainunitexternalid": None,
                          "mainunitexternalcode": None,
                          "importer": None,
                          "producer": None,
                          "alccode": None,
                          "waresexternalid": waresexternalid,
                          "waresexternalcode": None,
                          "country_name": None,
                          "wglevelnumber": None,
                          "waresgrid": None,
                          "wgname": None,
                          "wareskindcode": None,
                          "excise": None,
                          "warescode": warescode,
                          "wareskindname": None,
                          "parent_wgroup": None,
                          "buy_prices": None,
                          "sale_prices": None,
                          "min_sale_prices": min_sale_prices,
                          "barcodes": None}
            gwares_json.append({"wares": wares_json})
        return gwares_json

    def convert_min_prices(self, obj):
        """
        Конвертаци минимальной цены продажи
        @param obj: xml
        @return: json
        """

        sale_json = []

        shops_str = self.xml_value(obj, 'shop-indices')
        if shops_str is None or shops_str == '':
            # Для всех магазинов
            price = self.xml_attr_value(obj, 'value')
            begin_date = self.xml_value(obj, 'since-date')
            end_date = self.xml_value(obj, 'till-date')
            is_delete = self.xml_value(obj, 'deleted')
            week_day = self.xml_value(obj, 'days-of-week')
            if week_day is not None:
                week_day = [(self.week_days[name]) for name in str(week_day).split(' ')]
            begin_time = self.xml_value(obj, 'since-time')
            end_time = self.xml_value(obj, 'till-time')
            sale_json.append({"shop": None,
                              "sale_price": price,
                              "salerestrict": None,
                              "begin_date": begin_date,
                              "end_date": end_date,
                              'begin_time': begin_time,
                              'end_time': end_time,
                              'week_day': week_day,
                              'is_delete': '0' if is_delete == 'false' else '1'
                              })
        else:
            # Каждый магазин отдельно
            try:
                shops_str.split(',')
            except:
                return sale_json
            for itm in shops_str.split(','):
                shop_json = {"objexternalcode": itm,
                             "objemail": None,
                             "objinn": None,
                             "objkpp": None,
                             "objcode": None,
                             "objadress": None,
                             "objphone": None,
                             "objfsrarid": None,
                             "objname": None,
                             "objexternalid": None,
                             "objexternaltype": "D",
                             "objadressreal": None,
                             "objid": None,
                             "objtype": "C"
                             }
                price = self.xml_attr_value(obj, 'value')
                begin_date = self.xml_value(obj, 'since-date')
                end_date = self.xml_value(obj, 'till-date')
                is_delete = self.xml_value(obj, 'deleted')
                sale_json.append({"shop": shop_json,
                                  "sale_price": price,
                                  "salerestrict": '0' if is_delete == 'false' else '1',
                                  "begin_date": begin_date,
                                  "end_date": end_date
                                  })
        return sale_json

    @staticmethod
    def xml_value(xml, key):
        """
            Поиск значения по ключу
        """

        try:
            text = xml.find(key).text
        except:
            text = None
        if text:
            text = text.encode('cp1251', 'ignore')
            text = text.replace('\t', '')
            text = text.replace('\n', '')
        return text

    @staticmethod
    def xml_attr_value(xml, key):
        """
            Поиск значения по ключу
        """

        try:
            text = xml.attrib[key]
        except:
            text = None
        if text:
            text = text.encode('cp1251', 'ignore')
            text = text.replace('\t', '')
            text = text.replace('\n', '')
        return text
