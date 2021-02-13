# -*- coding: utf-8 -*-

"""
    Модуль преобразованиея CommerceML в json
"""

import json

from utils.xml import xml_value, xml_attrib
from rbsqutils import decodeUStr

__author__ = 'swat'


class CommerceML(object):
    """
        Клас для работы с файлами в формате CommerceML
    """

    parent_class = None

    file_name = None
    file_name_result = None
    xml_tree = None
    json_wgroup = []
    json_gwares = []
    json_price_type = []

    def __init__(self, parent_class, file_name):
        """
            Инициализация переменных
        """

        self.parent_class = parent_class
        self.file_name = file_name
        self.xml_load()

    def xml_load(self):
        """
            Загрузка xml
        """

        self.xml_tree = self.parent_class.parse_file_xml(self.file_name)

    def xml_parse(self):
        """
            Парсинг файла
        """

        if self.xml_tree:
            self.file_name_result = self.file_name.replace('.xml', '.json')
            root = self.xml_tree.getroot()
            result = []
            data = []
            if root.find('Классификатор'):
                xml_catalog = root.find('Классификатор/Группы')
                if xml_catalog:
                    self.convert_category(xml_catalog)

                xml_type_price = root.findall('ПакетПредложений/ТипыЦен/ТипЦены')
                if xml_type_price:
                    self.json_price_type = self. convert_type_price(xml_type_price)
                    xml_gwares = root.findall('ПакетПредложений/Предложения/Предложение')
                    if xml_gwares:
                        self.json_gwares = self.convert_gwares(xml_gwares, with_price=True)

                xml_gwares = root.findall('Каталог/Товары/Товар')
                if xml_gwares:
                    self.json_gwares = self.convert_gwares(xml_gwares)

                data.append({'catalog': self.json_wgroup,
                             'price_type': self.json_price_type,
                             'gwares': self.json_gwares})
                result.append({'type_data': 'data', 'data': data})

            if root.find('Документ'):
                document = self.convert_document(root.findall('Документ'))
                data.append({'document': document})
                result.append({'type_data': 'document', 'data': data})
            result = decodeUStr(json.dumps(result, encoding='cp1251', indent=1).encode('cp1251'))
            self.parent_class.text_save_to_file(result, self.file_name_result)
            return self.file_name_result

    def convert_category(self, xml_catalog, higher_id=None):
        """
            Преобразование товарных групп
        """

        for xml in xml_catalog.findall('Группа'):
            wgroup_id = xml_value(xml, 'Ид')
            name = xml_value(xml, 'Наименование')
            self.json_wgroup.append({'guid': wgroup_id,
                                     'name': name,
                                     'higher': higher_id})
            xml_sub_cat = xml.find('Группы')
            if xml_sub_cat:
                self.convert_category(xml_sub_cat, wgroup_id)

    @staticmethod
    def convert_type_price(xml_type_price):
        """
            Преобразование типа цены
        """

        json_type_price = []
        for xml in xml_type_price:
            price_id = xml_value(xml, 'Ид')
            name = xml_value(xml, 'Наименование')
            currency = xml_value(xml, 'Валюта')
            tax_name = xml_value(xml, 'Налог/Наименование')
            in_sum = xml_value(xml, 'Налог/УчтеноВСумме')
            json_type_price.append({'guid': price_id,
                                    'name': name,
                                    'currency': currency,
                                    'tax_name': tax_name,
                                    'in_sum': in_sum})
        return json_type_price

    def convert_gwares(self, xml_gwares, with_price=False, with_document=False):
        """
            Преобразование товаров
        """

        json_gwares = []

        for xml in xml_gwares:
            wares_id = xml_value(xml, 'Ид')
            name = xml_value(xml, 'Наименование')
            articul = xml_value(xml, 'Артикул')
            barcode = xml_value(xml, 'Штрихкод')
            main_unit_short = xml_value(xml, 'БазоваяЕдиница/Пересчет/Единица')
            main_unit_name = xml_attrib(xml, 'БазоваяЕдиница', 'НаименованиеПолное')
            main_unit_code = xml_attrib(xml, 'БазоваяЕдиница', 'Код')
            factor = xml_value(xml, 'БазоваяЕдиница/Пересчет/Коэффициент')
            group_guid = xml_value(xml, 'Группы/Ид')
            json_price = []
            cargo_doc = []
            if with_price:
                xml_price = xml.findall('Цены/Цена')
                if xml_price:
                    json_price = self.get_wares_price(xml_price)
            if with_document:
                price = xml_value(xml, 'ЦенаЗаЕдиницу')
                amount = xml_value(xml, 'Количество')
                summa = xml_value(xml, 'Сумма')
                unit_code = xml_value(xml, 'Единица')
                tax_name = xml_value(xml, 'Налоги/Налог/Наименование')
                tax_in_sum = xml_value(xml, 'Налоги/Налог/УчтеноВСумме')
                tax_sum = xml_value(xml, 'Налоги/Налог/Сумма')
                cargo_doc.append({'price': price,
                                  'amount': amount,
                                  'summa': summa,
                                  'unit_code': unit_code,
                                  'tax_name': tax_name,
                                  'tax_in_sum': tax_in_sum,
                                  'tax_sum': tax_sum})
            taxs = self.get_wares_tax(xml)
            g_tax_name = None
            g_tax_value = None
            if taxs:
                g_tax_name = taxs[0]['name']
                g_tax_value = taxs[0]['value']
            json_gwares.append({'guid': wares_id,
                                'code': wares_id,
                                'name': name,
                                'articul': articul,
                                'barcode': barcode,
                                'main_unit_short': main_unit_short,
                                'main_unit_name': main_unit_name,
                                'main_unit_code': main_unit_code.rstrip(),
                                'factor': factor,
                                'specifications_unit': self.get_basic_unit_info(xml),
                                'group_guid': group_guid,
                                'tax_name': g_tax_name,
                                'tax_value': g_tax_value,
                                'specifications_wares': self.get_wares_info(xml),
                                'price_list': json_price,
                                'cargo_doc': cargo_doc})
        return json_gwares

    def convert_document(self, xml_document):
        """
            Конвертация документа
        """

        json_document = []
        for xml in xml_document:
            doc_id = xml_value(xml, 'Ид')
            number_doc = xml_value(xml, 'Номер')
            doc_date = xml_value(xml, 'Дата')
            type_doc = xml_value(xml, 'ХозОперация')
            role = xml_value(xml, 'Роль')
            currency = xml_value(xml, 'Валюта')
            exchange = xml_value(xml, 'Курс')
            doc_sum = xml_value(xml, 'Сумма')
            doc_time = xml_value(xml, 'Время')
            doc_tax_name = xml_value(xml, 'Налоги/Налог/Наименование')
            doc_tax_in_sum = xml_value(xml, 'Налоги/Налог/УчтеноВСумме')
            doc_tax_sum = xml_value(xml, 'Налоги/Налог/Сумма')
            json_document.append({'doc_id': doc_id,
                                  'number_doc': number_doc,
                                  'doc_date': doc_date,
                                  'type_doc': type_doc,
                                  'role': role,
                                  'currency': currency,
                                  'exchange': exchange,
                                  'doc_sum': doc_sum,
                                  'objects': self.document_object(xml.findall('Контрагенты/Контрагент')),
                                  'doc_time': doc_time,
                                  'doc_tax_name': doc_tax_name,
                                  'doc_tax_in_sum': doc_tax_in_sum,
                                  'doc_tax_sum': doc_tax_sum,
                                  'cargo': self.convert_gwares(xml.findall('Товары/Товар'), with_document=True)})
        return json_document

    @staticmethod
    def document_object(xml):
        """
            Получение обеъктов документа
        """

        result = []
        for itm in xml:
            result.append({'giud': xml_value(itm, 'Ид'),
                           'name': xml_value(itm, 'Наименование'),
                           'off_name': xml_value(itm, 'ОфициальноеНаименование'),
                           'role': xml_value(itm, 'Роль')})
        return result

    @staticmethod
    def get_basic_unit_info(xml):
        """
            Получение свойств базовой ед
        """

        result = []
        for itm in xml.findall('БазоваяЕдиница/Пересчет/ДополнительныеДанные/ЗначениеРеквизита'):
            result.append({'name': xml_value(itm, 'Наименование'),
                           'value': xml_value(itm, 'Значение')})
        return result

    @staticmethod
    def get_wares_info(xml):
        """
            Получение свойств базовой ед
        """

        result = []
        for itm in xml.findall('ЗначенияРеквизитов/ЗначениеРеквизита'):
            result.append({'name': xml_value(itm, 'Наименование'),
                           'value': xml_value(itm, 'Значение')})
        return result

    @staticmethod
    def get_wares_tax(xml):
        """
            Получение налоговых ставок
        """

        result = []
        for itm in xml.findall('СтавкиНалогов/СтавкаНалога'):
            result.append({'name': xml_value(itm, 'Наименование'),
                           'value': xml_value(itm, 'Ставка')})
        return result

    @staticmethod
    def get_wares_price(xml):
        """
            получение прайсллиста
        """

        result = []
        for itm in xml:
            result.append({'name': xml_value(itm, 'Представление'),
                           'price_guid': xml_value(itm, 'ИдТипаЦены'),
                           'price': xml_value(itm, 'ЦенаЗаЕдиницу'),
                           'currency': xml_value(itm, 'Валюта'),
                           'unit': xml_value(itm, 'Единица'),
                           'factor': xml_value(itm, 'Коэффициент')
                           })
        return result

a = CommerceML()
a.xml_parse()
