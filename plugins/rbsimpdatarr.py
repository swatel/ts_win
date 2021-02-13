# -*- coding: utf-8 -*-
# swat 14.02.2012
# version 1.0.0.0
# модуль импорта данных (объектов, товаров) из внешних систем (http://run-retail.ru/)
# модуль унаследован от основного модуля импорта данных

import elementtree.ElementTree as et

import krconst
import BasePlugin as BP
import plugins.rbsimpdata as impdata

import plugins.impdata.impwgroup_base as imp_wgroup

from  rbsqutils import check_number, empty_str_to_null, BarcodeToDic, str_to_bool_int


class Plugin(impdata.Plugin):
    def run(self):
        xmlfile = self.ParseFileXML(self.filenames)
        if self.result['result'] == krconst.plugin_error:
            return False

        # проверим есть ли параметры
        if not self.taskparamsxml and self.taskparamsxml == '':
            importalltags = '1'

        if importalltags == '1':
            noimportgwares = '0'
            noimportobj = '0'
        else:
            noimportgwares = self.ParserXML(self.taskparamsxml, 'noimportgwares')
            if not noimportgwares:
                noimportgwares = '0'
            noimportobj = self.ParserXML(self.taskparamsxml, 'noimportobj')
            if not noimportobj:
                noimportobj = '0'

        if noimportobj == '0':
            ## Для импорта справочника объектов
            # импорт контрагентов
            customers = xmlfile.find('customers')
            if customers is not None:
                self.ImportCustomers(customers)

            # импорт сотрудников
            mans = xmlfile.find('mans')
            if mans is not None:
                self.ImportMans(mans)

            # импорт подразделений
            departmens = xmlfile.find('departmens')
            if departmens is not None:
                self.ImportDepartmens(departmens)

            # импорт складов
            warehouses = xmlfile.find('warehouses')
            if warehouses is not None:
                self.ImportWarehouses(warehouses)

            # фирмы
            firms = xmlfile.find('firms')
            if firms is not None:
                self.ImportFirms(firms)

        ## Для импорта справочника товаров
        # импорт периодов
        periods = xmlfile.find('periods')
        if periods is not None:
            self.ImportPeriods(periods)

        # импорт налоговых ставок
        taxs = xmlfile.find('taxs')
        if taxs is not None:
            self.ImportTaxs(taxs)

        # ед измерения
        units = xmlfile.find('units')
        if units is not None:
            self.ImportUnits(units)

        # импорт товаров
        if noimportgwares:
            gwares = xmlfile.find('gwares')
            if gwares is not None:
                self.import_gwares(gwares)

    def ImportCustomers(self, customers):
        '''
            Импорт контрагентов
        '''

        for obj in customers:
            code = self.xml_get_value_by_attr(obj, 'guid')
            name = self.xml_get_value_by_attr(obj, 'name')
            deletemarker = self.xml_get_value_by_attr(obj, 'deletemarker')
            parent = self.xml_get_value_by_attr(obj, 'parent')
            parentcode = self.xml_get_value_by_attr(obj, 'parentcode')
            parentgroup = self.xml_get_value_by_attr(obj, 'parentgroup')
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                sqlparams = [name, code, parentcode, 'I', 'C', deletemarker]
                self.ImportCategorySQL(sqlparams, code)
            else:
                namefull = empty_str_to_null(self.xml_get_value_by_attr(obj, 'namefull'))
                address = empty_str_to_null(self.xml_get_value_by_attr(obj, 'address'))
                inn = check_number(empty_str_to_null(self.xml_get_value_by_attr(obj, 'inn')))
                edrpou = check_number(empty_str_to_null(self.xml_get_value_by_attr(obj, 'edrpou')))
                certificatenumber = check_number(empty_str_to_null(self.xml_get_value_by_attr(obj, 'certificatenumber')))

                sqlparams = [code, name, namefull,'I', 'C', parentcode, address, deletemarker, inn, edrpou, certificatenumber, parentgroup, None, None, None, None]
                self.ImportCustomersSQL(sqlparams, code, 'customer')

    def ImportMans(self, mens):
        '''
            Импорт сотрудников
        '''

        for obj in mens:
            code = self.xml_get_value_by_attr(obj, 'guid')
            name = self.xml_get_value_by_attr(obj, 'name')
            deletemarker = self.xml_get_value_by_attr(obj, 'deletemarker')
            parent = self.xml_get_value_by_attr(obj, 'parent')
            parentcode = self.xml_get_value_by_attr(obj, 'parentcode')
            parentgroup = self.xml_get_value_by_attr(obj, 'parentgroup')
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                sqlparams = [name, code, parentcode, 'I', 'M', deletemarker]
                self.ImportCategorySQL(sqlparams, code)
            else:
                sqlparams = [code, name, None,'I', 'M', parentcode, None, deletemarker, None, None, None, parentgroup, None, None, None, None]
                self.ImportCustomersSQL(sqlparams, code, 'man')

    def ImportDepartmens(self, departmens):
        '''
            Импорт подразделений
        '''

        for obj in departmens:
            code = self.xml_get_value_by_attr(obj, 'guid')
            name = self.xml_get_value_by_attr(obj, 'name')
            deletemarker = self.xml_get_value_by_attr(obj, 'deletemarker')
            parent = self.xml_get_value_by_attr(obj, 'parent')
            parentcode = self.xml_get_value_by_attr(obj, 'parentcode')
            parentgroup = self.xml_get_value_by_attr(obj, 'parentgroup')
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                sqlparams = [name, code, parentcode, 'I', 'D', deletemarker]
                self.ImportCategorySQL(sqlparams, code)
            else:
                sqlparams = [code, name, None,'I', 'D', parentcode, None, deletemarker, None, None, None, parentgroup, None, None, None, None]
                self.ImportCustomersSQL(sqlparams, code, 'department')

    def ImportWarehouses(self, warehouses):
        '''
            Импорт складов
        '''

        for obj in warehouses:
            code = self.xml_get_value_by_attr(obj, 'guid')
            name = self.xml_get_value_by_attr(obj, 'name')
            deletemarker = self.xml_get_value_by_attr(obj, 'deletemarker')
            parent = self.xml_get_value_by_attr(obj, 'parent')
            parentcode = self.xml_get_value_by_attr(obj, 'parentcode')
            parentgroup = self.xml_get_value_by_attr(obj, 'parentgroup')
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                sqlparams = [name, code, parentcode, 'I', 'W', deletemarker]
                self.ImportCategorySQL(sqlparams, code)
            else:
                sqlparams = [code, name, None,'I', 'W', parentcode, None, deletemarker, None, None, None, parentgroup, None, None, None, None]
                self.ImportCustomersSQL(sqlparams, code, 'warehouse')

    def ImportFirms(self, firms):
        '''
            Импорт фирм
        '''

        for obj in firms:
            code = self.xml_get_value_by_attr(obj, 'guid')
            name = self.xml_get_value_by_attr(obj, 'name')
            deletemarker = self.xml_get_value_by_attr(obj, 'deletemarker')
            parent = self.xml_get_value_by_attr(obj, 'parent')
            parentcode = self.xml_get_value_by_attr(obj, 'parentcode')
            parentgroup = self.xml_get_value_by_attr(obj, 'parentgroup')
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                sqlparams = [name, code, parentcode, 'I', 'F', deletemarker]
                self.ImportCategorySQL(sqlparams, code)
            else:
                namefull = empty_str_to_null(self.xml_get_value_by_attr(obj, 'namefull'))
                address = empty_str_to_null(self.xml_get_value_by_attr(obj, 'address'))
                inn = check_number(empty_str_to_null(self.xml_get_value_by_attr(obj, 'inn')))
                edrpou = check_number(empty_str_to_null(self.xml_get_value_by_attr(obj, 'edrpou')))
                certificatenumber = check_number(empty_str_to_null(self.xml_get_value_by_attr(obj, 'certificatenumber')))

                sqlparams = [code, name, namefull,'I', 'F', parentcode, address, deletemarker, inn, edrpou, certificatenumber, parentgroup, None, None, None, None]
                self.ImportCustomersSQL(sqlparams, code, 'firm')

    def import_gwares(self, gwares):
        '''
            Импорт товаров
        '''
        for obj in gwares:
            warescode = self.xml_get_value_by_attr(obj, 'warescode')
            waresname = self.xml_get_value_by_attr(obj, 'waresname')
            deletemarker = self.xml_get_value_by_attr(obj, 'deletemarker')
            parent = empty_str_to_null(self.xml_get_value_by_attr(obj, 'parent'))
            parentcode = empty_str_to_null(self.xml_get_value_by_attr(obj, 'parentcode'))
            if self.xml_get_value_by_attr(obj, 'group') == '1':
                gw = imp_wgroup.BaseWGroup()
                gw.parent_class = self
                gw.code = warescode
                gw.name = waresname
                gw.parent_code = parentcode
                gw.parent = parent
                gw.delete_marker = deletemarker
                gw.save()
            else:
                articul = empty_str_to_null(self.xml_get_value_by_attr(obj, 'articul'))
                tax = empty_str_to_null(self.xml_get_value_by_attr(obj, 'tax'))
                expirationvalue = self.xml_get_value_by_attr(obj, 'expirationvalue',  flag='N')
                expirationtype = self.xml_get_value_by_attr(obj, 'expirationtype',  flag='N')
                # добавляем товар
                sqlparams = [waresname, warescode, None, parentcode, articul, tax, deletemarker, 'I',
                             expirationtype, expirationvalue, parent, None, None]
                if self.ImportGwaresSQL(sqlparams, warescode) != krconst.kr_sql_error:
                    # забираем доп атрибуты товара + ШК
                    waresunits = obj.find('waresunits')
                    if waresunits is not None:
                        # находим основную ед измерения
                        self.ImportWareUnits(waresunits, warescode, 'mainunit', 1)
                        # находим упаковку
                        self.ImportWareUnits(waresunits, warescode, 'mainpack', 1)
                        # находим паллету
                        self.ImportWareUnits(waresunits, warescode, 'mainpallet', 1)