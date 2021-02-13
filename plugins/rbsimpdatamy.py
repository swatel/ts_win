# -*- coding: utf-8 -*-
# swat 30.03.2012
# version 1.0.0.0
# модуль импорта данных (товаров) из внешних систем для системы Мой Магазин

import elementtree.ElementTree as et

import krconst
import BasePlugin as BP

from rbsqutils import str_to_bool_int, empty_str_to_null, BarcodeToDic

#delete
import time

class Plugin(BP.BasePlugin):
    def run(self):
        xmlfile = self.ParseFileXML(self.filenames)
        if self.result['result'] == krconst.plugin_error:
            return False

        # ед измерения
        units = xmlfile.find('units')
        if units is not None:
            self.ImportUnits(units)
        # товары
        gwares = xmlfile.find('gwares')
        if gwares is not None:
            self.ImportGwares(gwares)

    def ImportUnits(self, units):
        for obj in units:
            codeunit =  self.xml_get_value_by_attr(obj, 'code')
            nameunit =  self.xml_get_value_by_attr(obj, 'name')
            res = self.ExecuteSQL('execute procedure RBS_Q_MY_UNIT_INSSEL(?,?,?,?)',
                                      sqlparams = [codeunit, nameunit, codeunit, 'I'],
                                      fetch='one',
                                      ExtVer=True)
            if res['status'] == krconst.kr_sql_error:
                self.LogFile('Код ед измерения:' + codeunit + krconst.kr_term_enter + krconst.kr_term_enter)


    def ImportGwares(self, gwares):
        for obj in gwares:
            warescode = self.xml_get_value_by_attr(obj, 'warescode')
            waresname = self.xml_get_value_by_attr(obj, 'waresname')
            deletemarker = str_to_bool_int(self.xml_get_value_by_attr(obj, 'deletemarker'))
            parent = empty_str_to_null(self.xml_get_value_by_attr(obj, 'parent'))
            parentcode = empty_str_to_null(self.xml_get_value_by_attr(obj, 'parentcode'))
            if self.xml_get_value_by_attr(obj, 'group') == ('Да'):
                res = self.ExecuteSQL('execute procedure RBS_Q_MY_WARESGROUP_INSSEL(?,?,?,?,?)',
                                      sqlparams = [waresname, warescode, parentcode, 'I', deletemarker],
                                      fetch='one',
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile('Код группы товара:' + parentcode + krconst.kr_term_enter + krconst.kr_term_enter)
            else:
                articul = empty_str_to_null(self.xml_get_value_by_attr(obj, 'articul'))
                tax = empty_str_to_null(self.xml_get_value_by_attr(obj, 'tax'))
                mainunit = self.xml_get_value_by_attr(obj, 'mainunit')
                if len(mainunit) > 0:
                    res = self.ExecuteSQL('execute procedure RBS_Q_MY_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?)',
                                          sqlparams = [waresname, warescode, mainunit, parentcode, None, 'I', None, None, None, articul, deletemarker, tax],
                                          fetch='one',
                                          ExtVer=True)
                    if res['status'] == krconst.kr_sql_error:
                        self.LogFile('Код товара:' + warescode + krconst.kr_term_enter + krconst.kr_term_enter)
                    else:
                        # находим ШК
                        waresbarcode = []
                        barcodes = obj.find('barcodes')
                        if barcodes is not None:
                            for barcode in barcodes:
                                waresbarcode = BarcodeToDic(waresbarcode, self.xml_get_value_by_attr(barcode, 'value'), self.xml_get_value_by_attr(barcode, 'unit'))
                            for itm in waresbarcode:
                                res = self.ExecuteSQL('select * from RBS_Q_MY_IMP_WARESBARCODE(?,?,?)',
                                              sqlparams = [warescode, itm['unit'], itm['barcode']],
                                              fetch='one',
                                              ExtVer=True)
                                if res['status'] == krconst.kr_sql_error:
                                    self.LogFile('Код товара:' + warescode + '. ШК '+ itm['barcode'] + krconst.kr_term_enter + krconst.kr_term_enter)
