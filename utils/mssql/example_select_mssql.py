# -*- coding: windows-1251 -*-
# coding=utf-8

import select_mssql as ms

__author__ = 'swat'

mssql = ms.Mssql('SES_LINUX')
mssql.mssql_connect()
if mssql.result == 0:
    sql_text = '''use SES
                  exec infokey_GetGoodsInfo @barcode = ?
               '''
    sql_params = ['2204578000303']
    res = mssql.mssql_execute_sql(sql_text,
                                  sql_params,
                                  fetch='many')
    if res['status'] in(2, 3):
        print res['message']
    else:
        if res['datalist']:
            for itm in res['datalist']:
                print itm.cursor_description
                print itm.GOODSNAME
        else:
            print 'Empty result'
else:
    print mssql.message
