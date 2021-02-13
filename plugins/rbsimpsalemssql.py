# -*- coding: utf-8 -*-
"""
    swat 15.01.2014
    version 0.0.2.0
    общий плагин импорта реализации из внешних MSSQL БД
    Важно: Продажа и возврат импортируется в один документ
"""

import krconst
import BasePlugin as BP

from rbsqutils import TimeStampToDateTime


class Plugin(BP.BasePlugin):
    def run(self):
        
        self.docid = None
        self.docidsale = None
        self.docidret = None

        qtparams = self.XMLGetAllParams(self.taskparamsxml, asdic=True)

        ''' получим из параметров задания code MSSQL БД
            если коннекта к БД нет выходим
        '''
        self.msconnect = self.MSSQLConnect(qtparams['codedb'])
        if not self.msconnect:
            return False
        
        ''' получим xml файл c текстом SQL запросов к БД чеков '''
        file_sql_text = self.ParseFileXML(qtparams['filesqltext'])
        if self.result['result'] == krconst.kr_result_pligin_error:
            return False
        
        ''' получим SQL тексты и параметры '''
        self.sqlparams = self.xml_get_all_params_from_file(file_sql_text.find('params'), asdic=True)
        
        ''' получим дату чеков, если она есть в задании '''
        docsale = None
        qparams = self.XMLGetAllParams(self.queueparamsxml, asdic=True)
        docsale = qparams['docsale']
        if docsale == '':
            docsale = None

        ''' получим сумму по чекам из RBS '''
        rbssumsale = self.ExecuteSQL('select * from RBQ_Q_GET_SALE_BY_CHECK(?)',
                                     sqlparams = [docsale],
                                     fetch='many',
                                     ExtVer=True)
        if rbssumsale['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_MSSQL_error_get_sales_from_rbs + krconst.kr_term_double_enter)
            return False
        else:
            ''' в цикле по магазинам проверям совпадают ли сумму в двух БД '''
            for itm in rbssumsale['datalist']:
                mssqlsumsale = self.get_sum_from_mssql(str(itm['docsale']), itm['externalobjcheckid'])
                ''' чеки в БД MSSQL существуют '''
                if mssqlsumsale:
                    self.docidsale = itm['docidsale']
                    self.docidret = itm['docidret']
                    ''' проверим отличаются ли данные в БД '''
                    self.docid = None
                    if abs(float(mssqlsumsale['sumsale']) - float(itm['sumsale'])) >= 0.01:
                        self.get_check_head_from_mssql(str(itm['docsale']), itm['externalobjcheckid'], itm['objid'], self.sqlparams['typesalecode'])
                        self.docidsale = self.docid

                    ''' Документ возврата импортируется вместе с продажей (отр кол-во) '''
                    #self.docid = None
                    #if abs(float(mssqlsumsale['sumret']) - float(itm['sumret'])) >= 0.01:
                    #    self.get_check_head_from_mssql(str(itm['docsale']), itm['externalobjcheckid'], itm['objid'],
                    #  self.sqlparams['typeretcode'])
                    #   self.docidret = self.docid
                    if self.docidsale or self.docidret:
                        ''' обновим реализацию '''
                        resstatus = self.ExecuteSQL('select * from RBS_Q_SALE_STATUS(?,?)',
                                                    sqlparams = [self.docidsale, self.docidret],
                                                    fetch='one',
                                                    ExtVer=True)
                        if resstatus['status'] == krconst.kr_sql_error:
                            self.LogFile(krconst.kr_message_MSSQL_error_save_status + krconst.kr_term_double_enter)
                        else:
                            self.LogFile(resstatus['datalist']['msg'], Terms=1, SaveLogDB=True)
        
    def get_sum_from_mssql(self, docsale, externalobjid):
        """
            получим продажи по чекам
        """

        dic = {}
        sql_text = self.sqlparams['sqlgetsum']
        sql_params = [docsale, self.sqlparams['typesalecode'], externalobjid]
        execsql = self.MSSQLExecuteSQL(sql_text,
                                       sqlparams=sql_params,
                                       db_local=self.msconnect,
                                       fetch='one')
        if execsql['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_MSSQL_error_get_sales_from_mssql + krconst.kr_term_double_enter)
            return None
        else:
            if execsql['datalist']:
                if execsql['datalist'].summa:
                    dic['sumsale'] = execsql['datalist'].summa
                else:
                    dic['sumsale'] = 0.0
            else:
                dic['sumsale'] = 0.0
                
        execsql = self.MSSQLExecuteSQL(self.sqlparams['sqlgetsum'], 
                                       sqlparams=[docsale, self.sqlparams['typeretcode'], externalobjid], 
                                       db_local=self.msconnect,
                                       fetch='one')
        
        if execsql['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_MSSQL_error_get_sales_from_mssql + krconst.kr_term_double_enter)
            return None
        else:
            if execsql['datalist']:
                if execsql['datalist'].summa:
                    dic['sumret'] = execsql['datalist'].summa
                else:
                    dic['sumret'] = 0.0
            else:
                dic['sumret'] = 0.0
        return dic
    
    def get_check_head_from_mssql(self, docsale, externalobjid, objid, doctype):
        """
            Получение и сохрание в БД шапок чеков из MSSQL
        """

        execsql = self.MSSQLExecuteSQL(self.sqlparams['sqlheadcheck'], 
                                       sqlparams=[docsale, externalobjid, doctype], 
                                       db_local=self.msconnect,
                                       fetch='many')
        
        if execsql['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_MSSQL_error_get_check_from_mssql % docsale + krconst.kr_term_double_enter)
            return None
        else:
            for check in execsql['datalist']:
                ''' сохраним шипку чека в БД '''
                savedoc = self.ExecuteSQL('select * from RBS_Q_SALE_DOCUMENT_SAVE(?,?,?,?,?,?,?)',
                                          sqlparams = [objid, check._DOCIDCASH, check._DOCTYPE, check._EXTERNALOBJCHECKID, TimeStampToDateTime(check._DOCDATE), check._NUMBER, check._DOCSUMM],
                                          fetch='one',
                                          ExtVer=True,
                                          auto_commit=False
                                          )
                if savedoc['status'] == krconst.kr_sql_error:
                    ''' ошибка сохранения шапки чека '''
                    self.LogFile(krconst.kr_message_MSSQL_error_save_document % check._NUMBER + krconst.kr_term_double_enter)
                    ''' откатим транзакцию '''
                    self.db.rollback()
                else:
                    ''' если docsalesid существует то импортировать чек не нужно '''
                    if not self.docid:
                        self.docid = savedoc['datalist']['docid']
                    if savedoc['datalist']['docsalesid']:
                        ''' получим позиции чека '''
                        if self.get_check_detail_from_mssql(check._DOCIDCASH, savedoc['datalist']['docsalesid']):
                            self.db.commit()
                        else:
                            self.db.rollback()
    
    def get_check_detail_from_mssql(self, docidcash, docsalesid):
        """
            Получение и сохрание в БД позиций чеков из MSSQL
        """

        result = True
        execsql = self.MSSQLExecuteSQL(self.sqlparams['sqldetailcheck'], 
                                       sqlparams=[docidcash], 
                                       db_local=self.msconnect,
                                       fetch='many')
        
        if execsql['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_MSSQL_error_get_check_detail_from_mssql % (docidcash) + krconst.kr_term_double_enter)
            return None
        else:
            for wares in execsql['datalist']:
                savecargo = self.ExecuteSQL('execute procedure RBS_Q_SALE_CARGO_SAVE(?,?,?,?,?)',
                                            sqlparams = [docsalesid, wares._WARESCODE, wares._AMOUNT, wares._PRICE, wares._SUMMA],
                                            fetch='none', 
                                            ExtVer=True,
                                            auto_commit=False
                                            )
                if savecargo['status'] == krconst.kr_sql_error:
                    ''' ошибка сохранения позиции чека '''
                    self.LogFile(krconst.kr_message_MSSQL_error_save_cargo % (docidcash, wares._WARESCODE) + krconst.kr_term_double_enter)
                    result = False
        return result