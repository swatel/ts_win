# -*- coding: utf-8 -*-
import krconst
import BasePlugin as BP

class Plugin(BP.BasePlugin):
    def run(self):
        if self.queueparamsxml:
            OBJ1ID = self.ParserXML(self.queueparamsxml, 'OBJ1ID')
            OBJ2ID = self.ParserXML(self.queueparamsxml, 'OBJ2ID')
            SCHDATE1 = self.ParserXML(self.queueparamsxml, 'SCHDATE1')
            if self.result['result'] == krconst.plugin_error:
                return False
            sql = 'execute procedure K_WSET_DOS_ORDER_CALC' + '(?,?,?)'
            res = self.ExecuteSQL(sql, sqlparams = (OBJ1ID, OBJ2ID, SCHDATE1,))
            if res[0] == krconst.kr_sql_error:
                self.LogFile(res[1])
        else:
            self.result['result'] = krconst.plugin_error
            self.LogFile(krconst.m_e_params_is_null)