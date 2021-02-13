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
        if not flag:
            self.LogFile('Невозможно определить алгоритм обработки массива')
            self.result['result'] = krconst.plugin_error
            return False
        
    def api_getsales(self, dic_sales):
        for itm in dic_sales:
            equipment_hash = itm['equipmenthash']
            for doc in itm['document']:
                if doc['cargo'] != [] and doc['cardnumber'] != 'null':
                    # проверка на пустой чек
                    res = self.ExecuteSQL('execute procedure API_CASH_DOCUMENTSALES_TEMP(?,?,?,?,?,?,?,?)',
                                          sqlparams=[equipment_hash, doc['docdate'], doc['number'], doc['cashiercode'], doc['cashtype'], doc['doctypecode'], doc['docid'], doc['cardnumber']],
                                          fetch='none',
                                          ExtVer=True)
                    if res['status'] == krconst.kr_sql_error:
                        self.LogFile(decodeXStr(res['message']))
                        self.LogFile(krconst.kr_term_enter + krconst.kr_term_enter)
                        return None