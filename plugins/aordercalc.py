# -*- coding: utf-8 -*-
import re

import krconst
import BasePlugin as BP

class Plugin(BP.BasePlugin):
    def run(self):
        if self.queueparamsxml:
            OBJBONDID = self.ParserXML(self.queueparamsxml, 'OBJBONDID')
            OBJ1ID = self.ParserXML(self.queueparamsxml, 'OBJ1ID')
            OBJ2ID = self.ParserXML(self.queueparamsxml, 'OBJ2ID')
            WSETID = self.ParserXML(self.queueparamsxml, 'WSETID')
            ORDERDATE1 = self.ParserXML(self.queueparamsxml, 'ORDERDATE1')
            ORDERDATE2 = self.ParserXML(self.queueparamsxml, 'ORDERDATE2')
            CURDATE = self.ParserXML(self.queueparamsxml, 'CURDATE')
            SCHDATE1 = self.ParserXML(self.queueparamsxml, 'SCHDATE1')
            SCHDATE2 = self.ParserXML(self.queueparamsxml, 'SCHDATE2')
            if self.result['result'] == krconst.plugin_error:
                return False
            sql = 'execute procedure AORDER_CALC' + '(?,?,?,?,?,?,?,?,?)'
            res = self.ExecuteSQL(sql, sqlparams = [self.queueid, OBJBONDID, OBJ1ID, OBJ2ID, WSETID, CURDATE, SCHDATE1, SCHDATE2, None])
            if res[0] == krconst.kr_sql_error:
                self.LogFile(res[1])
                if re.findall('deadlock|lock conflict', res[1]):
                    self.LogFile('Restart queue')
                    self.UpdateResult(krconst.plugin_restart)
            else:
                # проверка на создание консолидированного заказа
                sql = 'select * from RBS_Q_AORDER_GENCONSID' + '(?,?)'
                res = self.ExecuteSQL(sql, 
                                      sqlparams = [self.queueid, OBJ2ID],
                                      fetch='one', 
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile(res['message'])
                    if re.findall('deadlock|lock conflict', res['message']):
                        self.LogFile('Restart queue')
                        self.UpdateResult(krconst.plugin_restart)
                if res['status'] == krconst.kr_sql_ok and res['datalist']['RESULTCODE'] == '2':
                    self.LogFile('Что то не так с расчетом ' + str(self.queueid))
                    self.result['result'] = krconst.plugin_error
        else:
            self.result['result'] = krconst.plugin_error
            self.LogFile(krconst.m_e_params_is_null)