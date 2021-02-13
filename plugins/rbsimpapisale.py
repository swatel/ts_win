# -*- coding: utf-8 -*-
"""
    swat 13.08.2012
    version 1.0.0.0
    модуль импорта чеков, пришедшие через API
"""

import simplejson as json

import krconst
import BasePlugin as BP

from rbsqutils import decodeXStr


class Plugin(BP.BasePlugin):
    """
        класс импорта чеков, пришедшие через API
    """

    def run(self):
        dic_sales = self.queueparamsxml
        try:
            dic_sales = json.loads(dic_sales)
        except:
            self.LogFile('Неверная стуктура json массива')
            self.result['result'] = krconst.plugin_error
            return False
        flag = None
        try:
            dic_sales[0]['sumsale']
            flag = 'api_checksales'
        except:
            pass
        
        if not flag:
            try:
                dic_sales[0]['document']
                flag = 'api_getsales'
            except:
                pass
        
        if flag == 'api_getsales':
            self.api_getsales(dic_sales)
        if flag == 'api_checksales':
            self.api_checksales(dic_sales[0])
        if not flag:
            self.LogFile('Невозможно определить алгоритм обработки массива')
            self.result['result'] = krconst.plugin_error
            return False
        
    def api_getsales(self, dic_sales):
        cnt_doc_sales = 0
        for itm in dic_sales:
            equipment_hash = itm['equipmenthash']
            for doc in itm['document']:
                cnt_doc_sales = cnt_doc_sales + 1
                doc_sales_id = 0
                doc_date_recalc = None
                if doc['cargo'] != []:
                    # проверка на пустой чек
                    res = self.ExecuteSQL('select * from API_CASH_DOCUMENTSALES(?,?,?,?,?,?,?,?)',
                                          sqlparams=[equipment_hash, doc['docdate'], doc['number'], doc['cashiercode'], doc['cashtype'], doc['doctypecode'], doc['docid'], doc['cardnumber']],
                                          fetch='one',
                                          ExtVer=True)
                    if res['status'] == krconst.kr_sql_error:
                        self.LogFile(decodeXStr(res['message']))
                        self.LogFile(krconst.kr_term_enter + krconst.kr_term_enter)
                        return None
                    doc_sales_id = res['datalist']['DOCSALESID']
                    doc_date_recalc = res['datalist']['DOCDATERECALC']
                # проверка не пришел ли чек, не пробитый через ККМ
                if doc_sales_id > 0:
                    for position in doc['cargo']:
                        res = self.ExecuteSQL('execute procedure API_CASH_CARGOSALES(?,?,?,?,?)',
                                              sqlparams=[doc_sales_id, position['waresid'], position['amount'], position['price'], position['docsum']],
                                              fetch='none',
                                              ExtVer=True)
                        if res['status'] == krconst.kr_sql_error:
                            self.LogFile(decodeXStr(res['message']))
                            self.LogFile(krconst.kr_term_enter + krconst.kr_term_enter)
                            return None
                # пришел какой то чек за прошлый день
                if doc_date_recalc is not None:
                    res = self.ExecuteSQL('select * from API_CASH_SUMSALE(?,?,?,?)',
                                          sqlparams=[equipment_hash, doc_date_recalc, 0, 0],
                                          fetch='one',
                                          ExtVer=True)
                    if res['status'] == krconst.kr_sql_error:
                        self.LogFile(decodeXStr(res['message']))
                        self.LogFile(krconst.kr_term_enter + krconst.kr_term_enter)
                        return None
            # вызываем процедуру для заполнения реализации онлайн
            res = self.ExecuteSQL('select * from API_CASH_SUMSALE(?,?,?,?)',
                                  sqlparams=[equipment_hash, None, 0, 0],
                                  fetch='one', 
                                  ExtVer=True)
            if res['status'] == krconst.kr_sql_error:
                self.LogFile(decodeXStr(res['message']))
                self.LogFile(krconst.kr_term_enter + krconst.kr_term_enter)
                return None
            
    def api_checksales(self, dicsales):
        sumsale = dicsales['sumsale']
        sumret = dicsales['sumret']
        if dicsales['sumsale'] == 'NULL':
            sumsale = None
        if dicsales['sumret'] == 'NULL':
            sumret = None    
        res = self.ExecuteSQL('select * from API_CASH_SUMSALE(?,?,?,?)',
                              sqlparams=[dicsales['equipmenthash'], dicsales['docdate'], sumsale, sumret], 
                              fetch='one',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(decodeXStr(res['message']))
            self.LogFile(krconst.kr_term_enter + krconst.kr_term_enter)
            return None