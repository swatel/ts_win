# -*- coding: utf-8 -*-
"""
    swat 26.10.2015
    version 0.0.3.0
    Импорт данных из Rarus
"""

import os

import krconst as c
import BasePlugin as Bp

import plugins.impdata.impwgroup_base as imp_wgroup
import plugins.impdata.imp_gwares_base as imp_gwares
import plugins.impdata.gwares.impunit as imp_unit
import plugins.impdata.imp_barcode_base as imp_barcode

from rbsqutils import unpack_file

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '26.10.2015'


class Plugin(Bp.BasePlugin):
    """
        класс импорта данных из Rarus
    """

    def run(self):
        # проверим формат файла
        if (os.path.basename(self.filenames)).split('.')[1] in ('7z', 'zip'):
            # распакуем файл
            res_unpack_file = unpack_file(self.filenames)
            if res_unpack_file['file_name']:
                self.filenames = res_unpack_file['file_name']
            else:
                self.result['result'] = c.plugin_error
                message = c.m_e_unpack_file % self.filenames
                self.log_file(message,
                              terms = 2,
                              save_log_db=True)
                return False

        xml_file = self.parse_file_xml(self.filenames)
        if self.result['result'] == c.plugin_error:
            return False

        # проверим что находиться в файле
        goods_catalog = xml_file.getroot()
        if goods_catalog.tag == 'goods-catalog':
            self.import_gwares(goods_catalog)
        else:
            # данная ошибка не является ошибкой выполнения плагина
            message = c.m_e_i_not_need_data_in_file % self.filenames
            self.log_file(message,
                          terms = 2,
                          save_log_db=True)
            return False

        # справочник товаров
        gwares = goods_catalog.findall('good')
        if gwares:
            self.import_gwares(gwares)

    def import_gwares(self, gwares):
        """
            Импорт товаров
        """

        for obj in gwares:
            # сначала сделаем импорт групп товаров
            wgroups = obj.find('group')
            wgroup_external_id = self.import_wgroup(wgroups)

            # ед измерения
            units = obj.find('measure-type')
            unit_external_id = self.import_unit(units)

            # товары
            g = imp_gwares.BaseIGwares()
            g.parent_class = self
            g.name = self.xml_value(obj, 'name')
            g.code = self.xml_attr_value(obj, 'marking-of-the-good')
            g.main_unit = unit_external_id
            g.second_unit = None
            g.parent_code = None
            g.articul = None
            g.tax = self.xml_value(obj, 'vat')
            g.delete_marker = None
            g.expiration_type = None
            g.expiration_value = None
            g.parent = None
            g.external_id = self.xml_attr_value(obj, 'marking-of-the-good')
            g.group_id = wgroup_external_id
            g.factor = None

            g.save()

            if g.wares_id:
                # сохраним свойства товара
                self.import_property(g.wares_id, obj)

                # сохраним ШК
                self.import_barcode(self.xml_attr_value(obj, 'marking-of-the-good'),
                                    unit_external_id,
                                    obj)

                # запишем цену товара
                self.import_price(g.wares_id, obj)

    def import_wgroup(self, wgroups):
        """
            Импорт товарных групп
        """

        result = None

        obj_parent = wgroups.find('parent-group')
        g = imp_wgroup.BaseWGroup()
        g.parent_class = self
        g.name = self.xml_value(wgroups, 'name')
        g.code = None
        g.parent_code = None
        g.delete_marker = None
        g.external_id = wgroups.attrib['id']
        if obj_parent:
            g.group_id = obj_parent.attrib['id']
            g.parent = None
        g.save()

        #result = g.wgroup_id

        if obj_parent:
            self.import_wgroup(obj_parent)

        return wgroups.attrib['id']

    def import_unit(self, obj):
        """
            Импорт ед измерения
        """

        u = imp_unit.Unit(self)
        u._external_id = self.xml_attr_value(obj, 'id')
        u._short_name = self.xml_value(obj, 'name')
        u.save()
        return self.xml_attr_value(obj, 'id')

    def import_property(self, waresid, obj):
        """
            Импорт свойств товара
        """

        sql_text = 'execute procedure Q_IMP_GWARES_PROPERTY_RARUS(?,?,?,?)'

        alcohol = None
        volume = None
        for itm in obj.findall('plugin-property'):
            key = self.xml_attr_value(itm, 'key')
            if key == 'alcoholic-content-percentage':
                alcohol = self.xml_attr_value(itm, 'value')
            if key == 'volume':
                volume = self.xml_attr_value(itm, 'value')

        type_wares = self.xml_value(obj, 'product-type')
        sql_params = [waresid, alcohol, volume, type_wares]

        res = self.execute_sql(sql_text,
                               sql_params=sql_params,
                               fetch='none')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка импорта свойств товара ' + str(waresid))

    def import_barcode(self, wares_code, unit, obj):
        """
            Импорт ШК
        """
        barcode = ''
        for itm in obj.findall('bar-code'):
            barcode = barcode + ' ' + self.xml_attr_value(itm, 'code')

        b = imp_barcode.BaseBarcode()
        b.parent_class = self
        b.code = wares_code
        b.barcode = barcode
        b.unit = unit
        b.save()

    def import_price(self, waresid, obj):
        """
            Импорт цен
        """

        sql_text = 'execute procedure Q_IMP_SALEPRICE_RARUS(?,?,?,?)'
        shop_str = self.xml_value(obj, 'shop-indices')
        obj_price = obj.find('price-entry')
        price = self.xml_attr_value(obj_price, 'price')
        is_delete = self.xml_value(obj, 'delete-from-cash')
        if is_delete == 'false':
            is_delete = '0'
        else:
            is_delete = '1'

        sql_params = [waresid, shop_str, price, is_delete]
        res = self.execute_sql(sql_text,
                               sql_params=sql_params,
                               fetch='none')
        if res['status'] == c.kr_sql_error:
            self.log_file('Ошибка импорта цен товара ' + str(waresid))

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
