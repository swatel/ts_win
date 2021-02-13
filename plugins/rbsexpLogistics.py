# -*- coding: utf-8 -*-
# swat 29.07.2013
# модуль экспорта данных в систему Logistics

import krconst
import BasePlugin as BP

class Plugin(BP.BasePlugin):
    def run(self):
        # соединимся с БД логистики 
        self.msconnect = self.MSSQLConnect('ExpImpLogistic')
        # если коннекта к БД нет выходим
        if not self.msconnect:
            return False
        
        self.ExportAuto()
        self.ExportShop()
        self.ExportDocument()
        
    def ExportAuto(self):
        '''
            Экспорт автомобилей в БД импорта
        '''
        
        auto = self.ExecuteSQL('select * from RBS_Q_AUTO_EXP',
                                sqlparams = [],
                                fetch='many', 
                                ExtVer=True)
        if auto['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_Logistic_error_get_auto + krconst.kr_term_double_enter)
            return False
        else:
            for itm in auto['datalist']:
                execsql = self.MSSQLExecuteSQL('INSERT INTO [Car] ([IdCar], [Numb], [WeightCap], [VolumeCap]) VALUES (?,?,?,?)', 
                                               sqlparams=[itm['aid'], itm['numauto'], itm['weightcap'], itm['volumecap']], 
                                               db_local=self.msconnect,
                                               fetch='none')
                if execsql['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.kr_message_Logistic_error_save_auto % (itm['numauto']) + krconst.kr_term_double_enter)
                    
    def ExportShop(self):
        '''
            Экспорт магазинов
        '''
        
        shop = self.ExecuteSQL('select * from RBS_Q_INSTALL_LISTSHOPS',
                                sqlparams = [],
                                fetch='many', 
                                ExtVer=True)
        if shop['status'] == krconst.kr_sql_error:
            self.LogFile('Ошибка импорта магазинов')
            return False
        else:
            for itm in shop['datalist']:
                execsql = self.MSSQLExecuteSQL('INSERT INTO Object(IdObject ,Name) VALUES (?,?)', 
                                               sqlparams=[itm['objcode'], itm['objfullname']], 
                                               db_local=self.msconnect,
                                               fetch='none')
                if execsql['status'] == krconst.kr_sql_error:
                    self.LogFile('Ошибка импорта магазинов')
                    
    def ExportDocument(self):
        '''
            Экспорт документов
        '''
        doc = self.ExecuteSQL('select * from RBS_Q_IMPDOC_LOGISTIC(?)',
                                sqlparams = ['13.03.2013'],
                                fetch='many', 
                                ExtVer=True)
        if doc['status'] == krconst.kr_sql_error:
            self.LogFile('Ошибка импорта магазинов')
            return False
        else:
            for itm in doc['datalist']:
                execsql = self.MSSQLExecuteSQL('INSERT INTO Document(IdDoc, Weight, Volume, Id_Base, Id_Object) VALUES (?,?,?,?,?)', 
                                               sqlparams=[itm['DOCIDCODE'], itm['WEIGHT'], itm['VOL'], '210', itm['codeshop']], 
                                               db_local=self.msconnect,
                                               fetch='none')
                if execsql['status'] == krconst.kr_sql_error:
                    self.LogFile('Ошибка импорта документов')
                                