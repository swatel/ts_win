# -*- coding: utf-8 -*-
# общий плагин импорта реализации из внешних БД
# O - онлайн
# C - куб
# M - маркет +

import krconst
import rbsqutils as ku
import BasePlugin as BP

class Plugin(BP.BasePlugin):
    def run(self):
       pass
                
    def GetSumSaleInOtherDB():
        pass
    
    
# ms = self.MSSQLConnect('MARKET')
       # if ms:
       #     res = self.MSSQLExecuteSQL('SELECT [UNITID] ,[UNITNAME] FROM [market].[dbo].[Exch_MP_UNIT]', sqlparams=[], db_local = ms)
       #     for items in res['datalist']:
       #         self.LogFile(items.UNITNAME)