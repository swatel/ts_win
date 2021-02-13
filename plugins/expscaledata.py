# -*- coding: utf-8 -*-

import krconst
import BasePlugin as BP

import time

class Plugin(BP.BasePlugin):
    def run(self):
        exporttype = self.ParserXML(self.queueparamsxml, 'EXPORTTYPE')
        if self.result['result'] == krconst.plugin_error:
            return False
        if exporttype in ('MARKET'):
            if exporttype =='MARKET':
                self.MainExportScaleToMarket()
        else:
            self.LogFile(krconst.kr_message_error_unsupportedexport % exporttype)
            self.UpdateResult(krconst.plugin_restart)
    
    def MainExportScaleToMarket(self):
        objid = self.ParserXML(self.queueparamsxml, 'OBJID')
        scaleid = self.ParserXML(self.queueparamsxml, 'SCALEID')
        if self.result['result'] == krconst.plugin_error:
            return False
        sql = 'select * from K_Q_EXPORTSCALEMARKET(?,?)'
        expdata = self.ExecuteSQL(sql, sqlparams=[scaleid, objid])
        if expdata[0] == krconst.kr_sql_error:
            self.LogFile(expdata[1])
        else:
            msmarket = self.MSSQLConnect('MARKET')
            msmarketds = self.MSSQLConnect('MARKETDS')
            for data in expdata[2]:
                if data['COND']:
                    if data['DBCOND'] == 'MARKET':
                        cond = self.MSSQLExecuteSQL(data['COND'], db_local=msmarket)
                        if cond['status'] == krconst.kr_sql_error:
                            self.LogFile(cond['message'])
                        else:
                            if cond['datalist'] != []:
                                execsql = self.MSSQLExecuteSQL(data['sql'], db_local=msmarket, auto_commit=False)
                                if execsql['status'] == krconst.kr_sql_error:
                                    self.LogFile(execsql['message'])
                    if data['DBCOND'] == 'MARKETDS':
                        cond = self.MSSQLExecuteSQL(data['COND'], db_local=msmarketds)
                        if cond['status'] == krconst.kr_sql_error:
                            self.LogFile(cond['message'])
                        else:
                            if cond['datalist'] != []:
                                print(data['sql'])
                                execsql = self.MSSQLExecuteSQL(data['sql'], sqlparams=[cond['datalist'][0][0], cond['datalist'][0][1]], db_local=msmarket, auto_commit=False)
                                if execsql['status'] == krconst.kr_sql_error:
                                    self.LogFile(execsql['message'])
                else:
                    #print 'sql', data['sql'] 
                    execsql = self.MSSQLExecuteSQL(data['sql'], db_local=msmarket, auto_commit=False)
                    if execsql['status'] == krconst.kr_sql_error:
                        self.LogFile(execsql['message'])
                if self.result['result'] == krconst.plugin_ok:
                    msmarket.commit()
                    msmarketds.commit()
                else:
                    msmarket.rollback()
                    msmarketds.rollback()
