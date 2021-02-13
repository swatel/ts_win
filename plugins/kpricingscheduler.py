# -*- coding: utf-8 -*-

import datetime
import time

import krconst
import BasePlugin as BP
import queue_db as db
import rbsqutils as rqu

class Plugin(BP.BasePlugin):
    def run(self):
        self.calcComparision()

    ## начал менять код под робота
    # заимпортишь, чего тебе не хватает - import datetime например
    # и не забудь замочить все .exposed = True они нужны были для моих тестов
    # import datetime
    # главный метод - calcComparision

    def GetCurDate(shortYear=None):
        today = datetime.date.today()
        if shortYear: today_Y= today.strftime("%y")
        else: today_Y =today.strftime("%Y")
        return (today.strftime("%d")+ "." + today.strftime("%m")+ "." + today_Y)

    def iif(condition, trueValue, falseValue):
        "if condition is true, return trueValue, else return falseValue"
        if condition:
            return trueValue
        else:
            return falseValue

    def CreateTask(self, **kwargs):
        t = kwargs.get('transaction', self)
        wsetid = kwargs.get('wsetid', None)
        modelid = kwargs.get('modelid', None)
        procname = kwargs.get('procname', 'K_PRICING_GLOBAL_CALC')
        starttime = iif(ajaxValidate(kwargs['starttime']), kwargs['starttime'], GetCurDate())
        actiondate = iif(ajaxValidate(kwargs['actiondate']), kwargs['actiondate'], GetCurDate())

        if 'anyway' in kwargs:
            sql='select * from K_QUEUE_CREATE_TASK_PRICE(?,?,?,?,?,?,?,?,?)'
            if 'direct_assortment_insert' in kwargs and kwargs['direct_assortment_insert'] == 1:
                sql='select * from K_QUEUE_CREATE_TASK_PRICE_STAT1(?,?,?,?,?,?,?,?,?)'
            #res = t.dbExec(sql,
            #    params=[
            #        starttime,
            #        self.getUserVar('userfio'),
            #        kwargs['priority'],
            #        kwargs['objid'],
            #        kwargs['waresid'],
            #        actiondate,
            #        wsetid,
            #        modelid,
            #        procname
            #        ], fetch='one')
            res = self.db.dbExec(sql,
                params=(
                    starttime,
                    self.getUserVar('userfio'),
                    kwargs['priority'],
                    kwargs['objid'],
                    kwargs['waresid'],
                    actiondate,
                    wsetid,
                    modelid,
                    procname,
                    ), fetch='one',auto_commit=False)
        else:
            sql='execute procedure K_PRICING_CHANGES_UPDATE(?,?,?,?)'
            #res = t.dbExec(sql,
            #    params=[
            #        wsetid,
            #        kwargs['waresid'],
            #        kwargs['objid'],
            #        starttime
            #       ], fetch='none')
            res = self.db.dbExec(sql,
                params=(
                    wsetid,
                    kwargs['waresid'],
                    kwargs['objid'],
                    starttime,
                    ), fetch='none',auto_commit=False)
        #return self.pyDumps({'res':'ok'})
        return True

    def getObjectsForCompartion(self, modelid, daynum=None, flag=None):
        "сериализация списка магазинов для просчета"
        #res = self.dbExec(sql='select * from K_PRICING_CALC_SCHEDULE_OBJECTS(?,?,?)',params=[modelid, daynum, flag], fetch='one')
        res = self.ExecuteSQL('select * from K_PRICING_CALC_SCHEDULE_OBJECTS(?,?,?)',sqlparams=(modelid, daynum, flag,))[2]
        if flag=="S":
            return res[0]['OBJECTS'][:-1]
        else:
            return res


    def getModelsForComparision(self, daynum=None):
        """
            модели для расчета в пределах дня
        """
        #return self.dbExec(sql='select * from K_PRICING_CALC_SCHEDULE_MODELS(?)',params=[daynum,], fetch='all')['datalist']
        return self.ExecuteSQL('select * from K_PRICING_CALC_SCHEDULE_MODELS(?)',sqlparams=(daynum,))[2]

    def getSetsForComparision(self, modelid, daynum=None):
        """
            наборы модели для расчета в пределах дня
        """
        #return self.dbExec(sql='select * from K_PRICING_CALC_SCHEDULE_WSETS(?,?)', params=[modelid, daynum], fetch='all')['datalist']
        return self.ExecuteSQL('select * from K_PRICING_CALC_SCHEDULE_WSETS(?,?)', sqlparams=(modelid, daynum,))[2]

    def checkScheme(self, oldprice, newprice):
        #return self.dbExec('select * from K_PRICING_CALC_LIMIT_FILTER(?,?)',[oldprice, newprice],fetch='one')['RES']
        return self.ExecuteSQL('select * from K_PRICING_CALC_LIMIT_FILTER(?,?)',sqlparams=(oldprice, newprice,))[2][0]['RES']


    def calcComparision(self, daynum=None):
        """
            Просчет переоценок
        """
        # удалим предыдущий расчет
        #self.dbExec('execute procedure K_PRICING_RECALC_IS_ON_DEL',[],fetch='none')
        self.ExecuteSQL('execute procedure K_PRICING_RECALC_IS_ON_DEL')
        # начинаем просчет, проход по моделям
        for model in self.getModelsForComparision(daynum):
            modelid = model['MODELID']
            # магазины модели (строкой)
            objects_str = self.getObjectsForCompartion(modelid, daynum, 'S')
            # наборы модели с учетом исключений
            sets = self.getSetsForComparision(modelid, daynum)
            # плоский список позиций с учетом логов изменений (исключая не изменявшиеся)
            wares = self.ajaxWaresForComparisionEx(sets, objects_str, 1)
            #print 'wares',wares
            # начинаем просчет
            for waresitem in wares:
                compare = self.ajaxPriceComparision(waresitem['waresid'], waresitem['objid'], '0', '0', '1')
                #print 'compare', compare
                #print 'sleep'
                #time.sleep(60)
                # если в цене найдены отличия - применим цену
                if compare['waresid'] is not None:
                    #print 'compare',compare
                    # проверим схему исключений
                    if self.checkScheme(compare['real'], compare['must']) == '1':
                        #self.dbExec('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,?)',
                        #            [modelid, waresitem['objid'], waresitem['waresid'], compare['real'], compare['must'], '0', 'K', ''],
                        #            fetch='none')
                        self.ExecuteSQL('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,?)',
                                    sqlparams=(modelid, waresitem['objid'], waresitem['waresid'], compare['real'], compare['must'], '0', 'K', '',))

                    else:
                        self.ajaxCreateTaskForComparedWaresEach(
                                                            modelid,
                                                            waresitem['objid'],
                                                            waresitem['waresid'],
                                                            compare['must'],
                                                            compare['real'])





    def ajaxWaresForComparisionEx(self, pos, objects='', wareschange=1):
        """
            разворачивает рекурсивно наборы и возвращает список товарных позиций
            с учетом изменений (т.е. отбрасывает те, что не менялись с прошлого пересчета)
        """
        if objects =='': return []
        wares = []
        params=[None, datetime.date.today().strftime('%d.%m.%Y'), None]
        if str(wareschange) == '1':
            sql = 'select distinct w.waresid, pc.objid from LISTWARESOFWARESSET_ALL(?,?,?) w \
                                        left join k_pricing_changes pc on pc.waresid = w.waresid \
                                                                      and pc.changedate <= ? \
                                                                      and pc.status = ? \
                                                                      and pc.objid in ('+objects+') \
                                        where pc.pricechangeid is not null'
            params.append(datetime.date.today().strftime('%d.%m.%Y'))
            params.append('0')
        else:
            sql = 'select * from LISTWARESOFWARESSET_ALL(?,?,?)'
        for item in pos:
            params[0] = item['WSETID']
            #res = self.dbExec(sql=sql, params=params, fetch='all')['datalist']
            res = self.ExecuteSQL(sql, sqlparams=params)[2]
            for w in res:
                if str(wareschange) == '1':
                    wares.append({'waresid': w['waresid'],'objid': w['objid']})
                else: wares.append({'waresid': w['waresid']})
        return wares

    def ajaxPriceComparision(self, waresid, objid, salerestrict, main, wareschange=0):
        if salerestrict == '0' or main == '0':
            #restrict = self.dbExec(sql='select * from K_PRICING_CHECK_SALERESTRICT(?,?,?,?)',
            #    params=[
            #        waresid,
            #        objid,
            #        salerestrict,
            #        main
            #        ],fetch='one')
            restrict = self.ExecuteSQL('select * from K_PRICING_CHECK_SALERESTRICT(?,?,?,?)',
                sqlparams=(
                    waresid,
                    objid,
                    salerestrict,
                    main,))[2][0]
            if (restrict['SALERESTRICT'] == '1' and salerestrict == '0') or (restrict['NOT_MAIN'] == '1' and main == '0'):
                #self.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',[waresid,objid],fetch='none')
                self.db.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',params=(waresid,objid,),fetch='none')
                return {'waresid':None}
        #res1 = self.dbExec(sql='select * from K_PRICING_WARESINFO(?, ?)',
        #    params=[
        #        waresid,
        #        objid
        #        ],fetch='one')
        res1 = self.ExecuteSQL('select * from K_PRICING_WARESINFO(?, ?)',
            sqlparams=(
                waresid,
                objid,))[2][0]
        try:
            #res2 = self.dbExec(sql='select * from K_PRICING_GLOBAL_CALC(?,?,?,?,?,?,?)',
            #        params=[
            #            objid,
            #            waresid,
            #            datetime.date.today().strftime('%d.%m.%Y'),
            #            None,
            #            None,
            #            'T',
            #            None
            #            ], fetch='one')
            res2 = self.ExecuteSQL('select * from K_PRICING_GLOBAL_CALC(?,?,?,?,?,?,?)',
                    sqlparams=(
                        objid,
                        waresid,
                        datetime.date.today().strftime('%d.%m.%Y'),
                        None,
                        None,
                        'T',
                        None,))
            if res2[0] == krconst.kr_sql_error:
                res2 = None
            else:
                res2 = res2[2][0]
        except:
            res2 = None
        if res1 is None or res2 is None:
            if str(wareschange) == '1':
                #self.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',[waresid,objid],fetch='none')
                self.db.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',params=(waresid,objid,),fetch='none')
            return {'waresid':None}
        #print 'res1[SALEPRICE]', res1['SALEPRICE']
        #print 'res2[SP_SALEPRICE]', res2['SP_SALEPRICE']
        if res1['SALEPRICE'] is None or res2['SP_SALEPRICE'] is None or (round(res1['SALEPRICE'],2) != round(res2['SP_SALEPRICE'],2)):
            return {'waresid':waresid,
                    'real':res1['SALEPRICE'],
                    'must':res2['SP_SALEPRICE']
                    }
        else:
            if str(wareschange) == '1':
                #self.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',[waresid,objid],fetch='none')
                self.db.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',params=(waresid,objid,),fetch='none')
            return {'waresid':None}

    def ajaxCreateTaskForComparedWaresEach(self, modelid, objid, waresid, price, oldprice):
        date = datetime.date.today().strftime('%d.%m.%Y')
        #t = self.trans()
        try:
            # апдейт ассортимента
            #t.dbExec('execute procedure K_PRICING_RECALCPRICE_TODAY(?,?,?)',[price,waresid,objid],fetch='none')
            #self.db.dbExec('execute procedure K_PRICING_RECALCPRICE_TODAY(?,?,?)',params=(price,waresid,objid,),fetch='none',auto_commit=False)
            # сброс изменения позиции
            #t.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',[waresid,objid],fetch='none')
            self.db.dbExec('UPDATE K_PRICING_CHANGES SET STATUS = 1 WHERE (WARESid = ? and objid =?)',params=(waresid,objid,),fetch='none',auto_commit=False)
            # лог в таблицу c флагом А - применено в ассортимент
            #t.dbExec('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,?)',
            #         [modelid, objid, waresid, oldprice, price, '1', 'A', ''],
            #         fetch='none')
            self.db.dbExec('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,?)',
                     params=(modelid, objid, waresid, oldprice, price, '0', 'A', '',),
                     fetch='none',auto_commit=False)
            #t.commit()
            self.db.commit()
        except:
            #t.rollback()
            self.db.rollback()

            #t2 = self.trans()
            try:
                self.CreateTask(
                    #transaction=t2,
                    transaction=None,
                    starttime=date,
                    priority=None,
                    objid=objid,
                    waresid=waresid,
                    actiondate=date,
                    anyway = 1,
                    )
                # лог в таблицу c флагом R - задание на просчет роботом
                #t2.dbExec('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,?)',
                #     [modelid, objid, waresid, oldprice, price, '1', 'R', ''],
                #     fetch='none')
                self.db.dbExec('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,?)',
                     params=(modelid, objid, waresid, oldprice, price, '0', 'R', '',),
                     fetch='none', auto_commit=False)
                #t2.commit()
                self.db.commit();

            except:
                # тут нужно поймать код ошибки
                errormessage = rqu.TracebackLog()
                #t2.rollback()
                self.db.rollback()
                # если уж совсем все плохо - кидаем ошибку в таблицу логов (по сути просто отложенная позиция с признаком Е)
                #t3 = self.trans()
                #t3.dbExec('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,?)',
                #     [modelid, objid, waresid, oldprice, price, '0', 'E', 'error code goes here'],
                #     fetch='none')
                #t3.commit()
                self.db.dbExec('execute procedure K_PRICING_RECALC_IS_ON_INS(?,?,?,?,?,?,?,substr(?,1,1023))',
                     params=(modelid, objid, waresid, oldprice, price, '0', 'E', errormessage,),
                     fetch='none')
### закончил