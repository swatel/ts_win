# -*- coding: utf-8 -*-
import krconst
import BasePlugin as BP


class Plugin(BP.BasePlugin):
    def run(self):
        if self.queueparamsxml:
            OBJBONDTYPE_CODE = self.ParserXML(self.queueparamsxml, 'OBJBONDTYPE_CODE')
            DateStatistic = self.ParserXML(self.queueparamsxml, 'Datestatistic')
            FLAGS = self.ParserXML(self.queueparamsxml, 'FLAGS');
            if self.result['result'] == krconst.plugin_error:
                return False
            sql = 'execute procedure AORDER_SHEDULER' + '(?,?,?,?)'
            res = self.ExecuteSQL(sql, sqlparams = (DateStatistic, OBJBONDTYPE_CODE, None, FLAGS,))
            if res[0] == krconst.kr_sql_error:
                self.LogFile(res[1])
            else:
                #if ((FLAGS == 'ACDF') or (FLAGS == 'ACDFR')):
                sql = 'execute procedure AO_QUEUE_CHECKSTAT' + '(?,?)'
                res = self.ExecuteSQL(sql, sqlparams = (self.queueid, 'S',))
                if res[0] == krconst.kr_sql_error:
                    self.LogFile(res[1])
        else:
            self.result['result'] = krconst.plugin_error
            self.LogFile(krconst.m_e_params_is_null)