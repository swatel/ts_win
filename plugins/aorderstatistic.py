# -*- coding: utf-8 -*-
import re
from mx.DateTime import today

import krconst
import BasePlugin as BP

class Plugin(BP.BasePlugin):
    def run(self):
        if self.queueparamsxml:
            FLAGS = self.ParserXML(self.queueparamsxml, 'FLAGS')
            if not FLAGS:
                StrStore = self.ParserXML(self.queueparamsxml, 'StoreID')
                FLAGS = 'A'
                CURDATE = self.ParserXML(self.queueparamsxml, 'CURDATE')
            else:
                OBJBONDID = self.ParserXML(self.queueparamsxml, 'OBJBONDID')
                OBJ1ID = self.ParserXML(self.queueparamsxml, 'OBJ1ID')
                OBJ2ID =self.ParserXML(self.queueparamsxml, 'OBJ2ID')
                WSETID = self.ParserXML(self.queueparamsxml, 'WSETID')
                CURDATE = self.ParserXML(self.queueparamsxml, 'CURDATE')
            if self.result['result'] == krconst.plugin_error:
                return False
            if FLAGS not in ('A', 'N'):
                self.result['result'] = krconst.plugin_error
                self.LogFile(krconst.kr_message_error_badflags % FLAGS)
            else:
                if CURDATE:
                    CURDATE = today()
                if FLAGS == 'A':
                    sqlTextStore = '''select OBGID from UR_INSTALL_OBJ left join getobjectname(obgid,'D') on  0=0 '''
                    if StrStore:
                        sqlTextStore = sqlTextStore + ''' where obgid in (''' + str(StrStore) + ''') '''
                    sqlTextStore = sqlTextStore + ''' order by fullname '''
                    resobj = self.ExecuteSQL(sqlTextStore, sqlparams = ())
                    if resobj[0] == krconst.kr_sql_error:
                        self.LogFile(resobj[1])
                    else:
                        sessionid = None
                        for obj in resobj[2]:
                            sql = 'select s.ID_SESSION_OUT from AORDER_MATRIX_RECALC' + '(?,?,?,?,?,?,?) s'
                            resStatistic = self.ExecuteSQL(sql, sqlparams = (CURDATE, obj['OBGID'], None, sessionid, None, None, FLAGS,))
                            if resStatistic[0] == krconst.kr_sql_error:
                                self.LogFile(resStatistic[1])
                            else:
                                sessionid = resStatistic[2]['ID_SESSION_OUT']
                if FLAGS == 'N':
                    sql = 'select s.ID_SESSION_OUT from AORDER_MATRIX_RECALC' + '(?,?,?,?,?,?,?) s'
                    resStatistic = self.ExecuteSQL(sql, sqlparams = (CURDATE, OBJ2ID, OBJ1ID, None, OBJBONDID, WSETID, FLAGS,))
                    if resStatistic[0] == krconst.kr_sql_error:
                        self.LogFile(resStatistic[1])                        
                        if re.findall('deadlock|lock conflict', resStatistic[1]):
                            self.LogFile('Restart queue')
                            self.UpdateResult(krconst.plugin_restart)
                            return False
                if FLAGS in ('A', 'N'):
                    sql = 'execute procedure AO_QUEUE_CHECKSTAT' + '(?,?)'
                    res = self.ExecuteSQL(sql, sqlparams = (self.queueid, FLAGS,))
                    if res[0] == krconst.kr_sql_error:
                        self.LogFile(res[1])
                        if re.findall('deadlock|lock conflict', res[1]):
                            self.LogFile('Restart queue')
                            self.UpdateResult(krconst.plugin_restart)
                            self.UpdateResult(krconst.plugin_restart)
