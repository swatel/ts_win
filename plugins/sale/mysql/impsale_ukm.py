# -*- coding: utf-8 -*-
"""
    swat 19.11.2014
    version 0.0.2.2
    Модуль импорта чеков из mysql UKM
"""


import krconst as c

import plugins.sale.impsale_base as s


class ImportUKM():
    """
        Класс импорта чеков
    """

    '''
        parent_class - ссылка на класс который унаследован от BasePlugin, что бы иметь доступ
        к необходимым методам
    '''

    parent_class = None

    db_cash_connect = None

    doc_id = None
    doc_id_sale = None
    doc_id_ret = None

    sale_rbs = None

    dict_doc_id = []

    def __init__(self, parent_class):
        """
            Запуск плагина
        """

        self.parent_class = parent_class

        self.sale_rbs = s.BaseImpSale(self.parent_class, 'UKM')

        '''
            получим из параметров задания code MySQL БД
            если коннекта к БД нет выходим
        '''
        self.db_cash_connect = self.parent_class.mysql_connect(self.sale_rbs.qt_params['code_db_UKM'])
        if not self.db_cash_connect:
            return

        ''' Получим продажи в RBS '''
        self.sale_rbs.sale_check_sum()

        if self.parent_class.result['result'] != c.plugin_error:
            ''' в цикле по магазинам проверяем совпадают ли сумму в двух БД '''
            for itm in self.sale_rbs.sale_list:
                ''' получаем сумму по магазин+дата в UKM '''
                sum_sale_cash = self.sale_rbs.get_sum_from_cash(self.db_cash_connect,
                                                                str(itm['doc_date_sale']),
                                                                itm['external_obj_id'])
                ''' чеки в БД кассы существуют '''
                if sum_sale_cash:
                    self.doc_id_sale = itm['doc_id_sale']
                    self.doc_id_ret = itm['doc_id_ret']

                    ''' проверим отличаются ли данные в БД '''
                    self.doc_id = None
                    if abs(float(sum_sale_cash['sum_sale']) - float(itm['sum_sale'])) >= 0.01:
                        header = self.sale_rbs.get_head_from_cash(self.db_cash_connect, itm)
                        if header:
                            self.save_head(header, itm['objid'])
                        self.doc_id_sale = self.doc_id

                    ''' Документ возврата импортируется вместе с продажей (отр кол-во) '''
                    #self.doc_id = None
                    #if abs(float(sum_sale_cash['sum_sale']) - float(itm['sum_sale'])) >= 0.01:
                    #    header = self.sale_rbs.get_head_from_cash(self.db_cash_connect, itm)
                    #if header:
                    #        self.save_head(header, itm['objid'])
                    #    self.doc_id_sale = self.doc_id
                    #todo перенести в общий класс
                    self.dict_doc_id.append({'doc_id_sale': self.doc_id_sale, 'doc_id_ret': self.doc_id_ret})
                    #self.sale_rbs.save_end(self.doc_id_sale, self.doc_id_ret)

    def save_head(self, header, obj_id):
        """
            сохрание в БД шапок чеков из кассы
        """

        for check in header:
            ''' сохраним шапку чека в БД '''
            doc = s.BaseImpSaleDoc(self.parent_class, 'UKM')
            doc.obj_id = obj_id
            doc.doc_id_cash = check.DOCIDCASH
            doc.doc_type_code = check.DOCTYPE.encode('cp1251')
            doc.external_obj_id = check.EXTERNALOBJCHECKID.encode('cp1251')
            doc.doc_date = check.DOCDATE
            doc.doc_number = str(check.NUMBER)
            doc.doc_sum = float(check.DOCSUMM)
            doc.save()

            if doc.result_save:
                self.doc_id = doc.doc_id
                if self.doc_id:
                    if doc.doc_sales_id:
                        ''' получим позиции чека '''
                        detail = self.sale_rbs.get_detail_from_cash(self.db_cash_connect,
                                                                    [check.DOCIDCASH,
                                                                     check.EXTERNALOBJCHECKID,
                                                                     check.CASHID])
                        if detail:
                            doc.result_save = self.save_detail(detail, doc.doc_sales_id)
                        else:
                            doc.result_save = False
                    doc.save_end()

    def save_detail(self, detail, doc_sales_id):
        """
            сохрание в БД позиций чека
        """

        result = True

        for wares in detail:
            cargo = s.BaseImpSaleCargo(self.parent_class, 'UKM')
            cargo.doc_sales_id = doc_sales_id
            cargo.wares_code = wares.WARESCODE.encode('cp1251')
            cargo.amount = float(wares.AMOUNT)
            cargo.price = float(wares.PRICE)
            cargo.summa = float(wares.SUMMA)
            cargo.save()

            result = cargo.result_save
        return result