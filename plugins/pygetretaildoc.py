# -*- coding: utf-8 -*-

import krconst
import BasePlugin as BP


import time

class Plugin(BP.BasePlugin):
    def run(self):
        onlinecfg = self.read_config_other_db('ONLINE')
        onlineDB = self.connect_other_db(onlinecfg)
        if onlineDB:
            zona = self.ParserXML(self.queueparamsxml, 'ZONA')
            if self.result['result'] == krconst.plugin_error:
                return False
            if zona: # найдем все дата+магаз что нужно будет проимпортировать
                sql = 'select * from RBS_Q_GETIMPORTBYZONA(?)'
                docs = self.ExecuteSQL(sql, sqlparams=[zona], db_local = onlineDB)
                if docs[0] == krconst.kr_sql_error:
                    self.LogFile(docs[1])
                else:
                    # создаем задание на импорт документов
                    sql = 'select * from K_IMPONLINE_CREATETASK(?,?,?)'
                    for doc in docs[2]:
                        task = self.ExecuteSQL(sql, sqlparams=[doc['objid'], doc['docdate'], self.quetaskid])
                        if task[0] == krconst.kr_sql_error:
                            self.LogFile(task[1])
                        else:
                            self.LogFile(krconst.kr_message_createtaskok % task[2][0]['RESULTP'])
            else:    # импорт дата+магаз
                docdate = self.ParserXML(self.queueparamsxml, 'Date')
                obj = self.ParserXML(self.queueparamsxml, 'StoreID')
                if self.result['result'] == krconst.plugin_error:
                    return False
                # обновим статус документа
                self.UpdateStatusDocOnline(obj, docdate, 1, onlineDB)
                # получим сумму реализации из БД онлайн
                sql = 'select * from RBS_Q_GETSUMBYOBJDATE(?,?)'
                docsum = self.ExecuteSQL(sql, sqlparams=[obj, docdate], db_local = onlineDB)
                if docsum[0] == krconst.kr_sql_error:
                    self.LogFile(docsum[1])
                else:
                    # делаем проверку на необходимость импорта
                    docidsale = 0
                    docidret = 0
                    flagsale = -1
                    flagret = -1
                    sql = 'select * from K_IMPMARKET_CHECKSALE(?,?,?,?)'
                    isimport = self.ExecuteSQL(sql, sqlparams=[obj, docdate, docsum[2][0]['sumsale'], docsum[2][0]['sumret']])
                    if isimport[0] == krconst.kr_sql_error:
                        self.LogFile(isimport[1])
                    else:
                        docidsale = isimport[2][0]['DOCIDSALE']
                        docidret = isimport[2][0]['DOCIDRET']
                        flagsale = int(isimport[2][0]['FLAGSALE'])
                        flagret = int(isimport[2][0]['FLAGRET']) 
                        self.docidimport = 0
                        # импортируем продажу
                        if flagsale == 1: # сумма в курсе больше суммы онлайна
                            if docidsale > 0:
                                # обнуление реализации
                                 if self.SaleClear(docidsale, 'PRESALE') == 1:
                                     self.LogFile(krconst.kr_message_error_firstclear % docidsale)
                                 else: 
                                     # импорт реализации
                                     self.ImportSale(obj, docdate, 0, onlineDB)
                                     # чистка данных
                                     if self.SaleClear(docidsale, 'CLEARSALE') == 1:
                                         self.LogFile(krconst.kr_message_error_firstclear % docidsale)
                        if flagsale == 2:
                            self.ImportSale(obj, docdate, 0, onlineDB)
                            # чистка данных}
                            if docidsale == 0:
                                docidsale = self.docidimport
                            if self.SaleClear(docidsale, 'CLEARSALE') == 1:
                                 self.LogFile(krconst.kr_message_error_firstclear % docidsale)
                        if flagsale == 3:
                            self.LogFile(krconst.kr_message_error_errorstatus % docidsale)
                             
                        # импортируем возврат
                        if flagret == 1: # сумма в курсе больше суммы онлайна
                            if docidret > 0:
                                # обнуление реализации
                                 if self.SaleClear(docidret, 'PRESALE') == 1:
                                     self.LogFile(krconst.kr_message_error_firstclear % docidret)
                                 else: 
                                     # импорт реализации
                                     self.ImportSale(obj, docdate, 1, onlineDB)
                                     # чистка данных
                                     if self.SaleClear(docidret, 'CLEARSALE') == 1:
                                         self.LogFile(krconst.kr_message_error_errorstatus % docidret)
                        if flagret == 2:
                            self.ImportSale(obj, docdate, 1, onlineDB)
                            # чистка данных}
                            if docidret == 0:
                                docidret = self.docidimport
                            if self.SaleClear(docidret, 'CLEARSALE') == 1:
                                 self.LogFile(krconst.kr_message_error_firstclear % docidret)
                        if flagret == 3:
                            self.LogFile(krconst.kr_message_error_firstclear % docidret)
                    if self.result['result'] == krconst.plugin_ok:
                        sql = 'select * from RBS_Q_GETSUMBYOBJDATE(?,?)'
                        docsum = self.ExecuteSQL(sql, sqlparams=[obj, docdate], db_local = onlineDB)
                        if docsum[0] == krconst.kr_sql_error:
                            self.LogFile(docsum[1])
                        else:
                        # делаем проверку на необходимость импорта
                            flagsale = -1
                            flagret = -1
                            sql = 'select * from K_IMPMARKET_CHECKSALE(?,?,?,?)'
                            isimport = self.ExecuteSQL(sql, sqlparams=[obj, docdate, docsum[2][0]['sumsale'], docsum[2][0]['sumret']])
                            if isimport[0] == krconst.kr_sql_error:
                                self.LogFile(isimport[1])
                            else:
                                flagsale = int(isimport[2][0]['FLAGSALE'])
                                flagret = int(isimport[2][0]['FLAGRET'])
                                if flagsale == 0 and flagret == 0:
                                    self.UpdateStatusDocOnline(obj, docdate, 2, onlineDB)
                            
    def SaleClear(self, docid, typeclear):
        sql = 'execute procedure K_Q_IMPORTSALE_CLEAR(?,?)'
        clearsql =  self.ExecuteSQL(sql, sqlparams=[typeclear, docid])
        if clearsql[0] == krconst.kr_sql_error:
            self.LogFile(clearsql[1])
            return 1
        else:
            return 0
        
    def ImportSale(self, objid, docdate, typesale, onlineDB):
        self.docidimport = 0
        docid = 0
        id_error = 0
        sql = 'select * from RBS_Q_GETDOCUMENT(?,?,?)'
        docsale = self.ExecuteSQL(sql, sqlparams=[objid, docdate, typesale], db_local = onlineDB)
        if docsale[0] == krconst.kr_sql_error:
            self.LogFile(docsale[1])
            id_error = 1
        else:
            if typesale == 0: MODESALE = 'SALE'
            if typesale == 1: MODESALE = 'RET'
            for pos in docsale[2]:
                sql = 'select * from K_Q_IMPORTSALE(?,?,?,?,?,?,?,?,?)'
                passave = self.ExecuteSQL(sql, sqlparams=[docid, str(pos['CODE']), pos['SALESPRICE'], pos['QUANTITY'], pos['SALESAMOUNT'], MODESALE, 1, objid, docdate])
                if passave[0] == krconst.kr_sql_error:
                    self.LogFile(passave[1] + ' ' + str(pos['CODE']))
                    id_error = 1
                else:
                    docid = passave[2][0]['new_docid']
                    if int(passave[2][0]['id_error']) == 1:
                        self.LogFile(passave[2][0]['ERR_MESSAGE'] + ' ' + str(pos['CODE']))
                        id_error = 1
            self.docidimport = docid            
            if id_error > 0: 
                return 1
            else:
                return 0
    
    def UpdateStatusDocOnline(self, objid, docdate, status, onlineDB):
        sql = 'execute procedure RBS_Q_UPDATEDOCUMENT(?,?,?)'
        updatedoc =  self.ExecuteSQL(sql, sqlparams=[objid, docdate, status], db_local = onlineDB)
        if updatedoc[0] == krconst.kr_sql_error:
            self.LogFile(updatedoc[1])
                
                
                