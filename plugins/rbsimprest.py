# -*- coding: utf-8 -*-

import elementtree.ElementTree as et

import krconst
import BasePlugin as BP

from  rbsqutils import decodeXStr

class Plugin(BP.BasePlugin):
    def run(self):
        xmlfile = self.ParseFileXML(self.filenames)
        if self.result['result'] == krconst.plugin_error:
            return False
        wrest = xmlfile.getroot()
        if len(wrest):
            daterest = self.xml_get_value_by_attr(wrest, 'daterest')
            for objrest in wrest:
                taskid = 0
                objcode = self.xml_get_value_by_attr(objrest, 'codeobject')
                objname = self.xml_get_value_by_attr(objrest, 'nameobject')
                objtype = self.xml_get_value_by_attr(objrest, 'typeobject')
                
                sql = 'select w.TASKID from RBS_Q_IMPREST(?,?,?,?,?,?,?,?,?,?) w '
                res = self.ExecuteSQL(sql, 
                                      sqlparams = [objcode, objname, objtype, daterest, None, None, None, None,'R', None],
                                      fetch='one', 
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile(decodeXStr(res['message']))
                    self.LogFile('Код объекта:' + objcode)
                
                if res['datalist']:
                    try:
                        taskid = int(res['datalist']['TASKID'])
                    except:
                        taskid = 0
                if taskid > 0:
                    for wares in objrest:
                        warescode = self.xml_get_value_by_attr(wares, 'warescode')
                        waresname = self.xml_get_value_by_attr(wares, 'waresname')
                        codeunit = self.xml_get_value_by_attr(wares, 'mainunit')
                        cntrest = self.xml_get_value_by_attr(wares, 'quantity', 'N')
                        sql = 'execute procedure RBS_Q_IMPREST(?,?,?,?,?,?,?,?,?,?)'
                        res = self.ExecuteSQL(sql, 
                                              sqlparams = [None, None, None, None, warescode, waresname, codeunit, cntrest, 'C', taskid],
                                              fetch='none', 
                                              ExtVer=True)
                        if res['status'] == krconst.kr_sql_error:
                            self.LogFile(decodeXStr(res['message']))
                            self.LogFile('Код товара:' + warescode)
        else:
            self.result['result'] = krconst.plugin_error
            self.LogFile(krconst.kr_message_error_errorxmlkeyimport % 'Остатки')