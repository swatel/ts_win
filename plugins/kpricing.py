# -*- coding: utf-8 -*-
import re
import time

import krconst
import BasePlugin as BP

class Plugin(BP.BasePlugin):
    def run(self):
        if self.queueparamsxml:
            OBJ1ID = self.ParserXML(self.queueparamsxml, 'OBJ1ID')
            AWARESID = self.ParserXML(self.queueparamsxml, 'AWARESID')
            ADATE =  self.ParserXML(self.queueparamsxml, 'ADATE')
            AWSETID =  self.ParserXML(self.queueparamsxml, 'AWSETID')
            AMODELID = self.ParserXML(self.queueparamsxml, 'AMODELID')
            APROCNAME =  self.ParserXML(self.queueparamsxml, 'APROCNAME')
            if self.result['result'] == krconst.plugin_error:
                return False
            sql = 'execute procedure ' + APROCNAME + '(?,?,?,?,?,?,?)'
            res = self.ExecuteSQL(sql, sqlparams = (OBJ1ID, AWARESID, ADATE, AWSETID, AMODELID, None, self.queueid,))
            if res[0] == krconst.kr_sql_error:
                #self.result = krconst.plugin_error
                if re.findall('deadlock|lock conflict', res[1]):
                    self.LogFile('Restart queue')
                    self.UpdateResult(krconst.plugin_restart)
            #if res[0] == krconst.kr_sql_ok:
            #    self.result = krconst.plugin_ok
        else:
            #self.LogPlugin('xml params is null', krconst.log_error)
            self.UpdateResult(krconst.plugin_error)