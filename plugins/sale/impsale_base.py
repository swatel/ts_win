# -*- coding: utf-8 -*-
"""
    swat 19.11.2014
    version 0.0.2.2
    Базовый модуль импорта чеков
    В одной БД может быть несколько магазинов
"""

import krconst as c

from rbsqutils import TimeStampToDateTime

VERSION = '0.0.2.2'


class BaseImpSale():
    """
        Базовый класс для импорта чеков
    """

    '''
        parent_class - ссылка на класс который унаследован от BasePlugin, что бы иметь доступ
        к необходимым методам, будем сокращать до p_c
    '''
    p_c = None

    doc_date = None
    cash_type = None

    sale_list = None

    ''' параметры импорта '''
    qt_params = None
    file_sql_text = None
    file_sql_params = None
    q_params = None
    q_obj_id = None

    def __init__(self, parent_class, cash_type=None):

        self.p_c = parent_class
        self.cash_type = cash_type

        self.get_config()

    def get_config(self):
        """
            Получение настроек импортп
        """

        self.qt_params = self.p_c.xml_get_all_params(self.p_c.taskparamsxml, as_dic=True)

        if self.qt_params['sql_locate'] == 'file':
            ''' получим xml файл c текстом SQL запросов к БД чеков '''
            self.file_sql_text = self.p_c.parse_file_xml(self.qt_params['file_sql_text' + '_' + self.cash_type])
            if self.file_sql_text:
                ''' получим SQL тексты и параметры '''
                self.file_sql_params = self.p_c.xml_get_all_params_from_file(self.file_sql_text.find('params'),
                                                                             as_dic=True)

                self.q_params = self.p_c.xml_get_all_params(self.p_c.queueparamsxml, as_dic=True)

                ''' получим дату чеков, если она есть в задании '''
                self.doc_date = self.q_params['docsale']
                if self.doc_date == '':
                    self.doc_date = None
                ''' Если есть объект в задании '''
                try:
                    self.q_obj_id = self.q_params['objid']
                except:
                    self.q_obj_id = None
                if self.q_obj_id == '':
                    self.q_obj_id = None

    def sale_check_sum(self):
        """
            Получение суммы продаж за день в RBS
        """

        sql_text = 'select * from RBQ_Q_GET_SALE_BY_CHECK(?,?,?)'
        sql_params = [self.doc_date, self.cash_type, self.q_obj_id]

        res = self.p_c.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch='many')
        if res['status'] == c.kr_sql_error:
            self.p_c.log_file(c.m_e_cash_get_sales_from_rbs + c.t_double_enter)
        else:
            self.sale_list = res['datalist']

    def get_sum_from_cash(self, db_cash_connect, docsale, external_obj_id):
        """
            получим продажи по чекам из кассы
            На данный момент для MSSQL и MySQL
        """

        dic = {}
        sql_text = self.file_sql_params['sql_get_sum']
        sql_params = [docsale, self.file_sql_params['type_sale_code'], external_obj_id]

        sum_cash = self.p_c.odb_exec_sql(sql_text,
                                         sql_params=sql_params,
                                         db_local=db_cash_connect,
                                         fetch='one')
        if sum_cash['status'] == c.kr_sql_error:
            self.p_c.log_file(c.m_e_cash_get_sales_from_cash % db_cash_connect.dn_name + c.t_double_enter)
            return None
        else:
            dic['sum_sale'] = 0.0
            if sum_cash['datalist']:
                print(sum_cash['datalist'])
                print('!!',sum_cash['datalist'])
                print('!!',sum_cash['datalist'].SUMMA)
                if sum_cash['datalist'].SUMMA: 
                    dic['sum_sale'] = sum_cash['datalist'].SUMMA

        sql_text = self.file_sql_params['sql_get_sum']
        sql_params = [docsale, self.file_sql_params['type_ret_code'], external_obj_id]

        sum_cash = self.p_c.odb_exec_sql(sql_text,
                                         sql_params=sql_params,
                                         db_local=db_cash_connect,
                                         fetch='one')

        if sum_cash['status'] == c.kr_sql_error:
            self.p_c.log_file(c.m_e_cash_get_sales_from_cash % db_cash_connect.dn_name + c.t_double_enter)
            return None
        else:
            dic['sum_ret'] = 0.0
            if sum_cash['datalist']:
                if sum_cash['datalist'].SUMMA:
                    dic['sum_sale'] = sum_cash['datalist'].SUMMA
        return dic

    def get_head_from_cash(self, db_cash_connect, itm):
        """
            Получение шапок чеков из БД кассы
        """

        doc_date_sale = str(itm['doc_date_sale'])
        external_obj_id = itm['external_obj_id']
        doc_type = self.file_sql_params['type_sale_code']

        head = self.p_c.odb_exec_sql(self.file_sql_params['sql_head_check'],
                                     sql_params=[doc_date_sale, external_obj_id, doc_type],
                                     db_local=db_cash_connect,
                                     fetch='many')

        if head['status'] == c.kr_sql_error:
            message = c.m_e_cash_get_head_from_cash % (db_cash_connect.dn_name, doc_date_sale) + c.t_double_enter
            self.p_c.log_file(message)
            return None
        else:
            return head['datalist']

    def get_detail_from_cash(self, db_cash_connect, sql_params):
        """
            Получение и сохрание в БД позиций чеков из БД кассы
        """

        detail = self.p_c.odb_exec_sql(self.file_sql_params['sql_detail_check'],
                                       sql_params=sql_params,
                                       db_local=db_cash_connect,
                                       fetch='many')

        if detail['status'] == c.kr_sql_error:
            message = c.m_e_cash_get_detail_from_cash % (db_cash_connect.dn_name, sql_params) + c.t_double_enter
            self.p_c.log_file(message)
            return None
        else:
            if not detail['datalist']:
                self.p_c.result['result'] = c.plugin_error
                self.p_c.log_file(c.m_e_cash_cargo_none % sql_params + c.t_double_enter)
            return detail['datalist']

    def save_end(self, doc_id_sale, doc_id_ret):
        """
            обновим реализацию на основе чеков
        """

        sql_text = 'select * from RBS_Q_SALE_QUEUE(?,?)'
        sql_params = [doc_id_sale, doc_id_ret]

        res = self.p_c.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch='one')
        if res['status'] == c.kr_sql_error:
            self.p_c.log_file(c.m_e_cash_status + c.t_double_enter)
        else:
            self.p_c.log_file(res['datalist']['msg'], terms=1, save_log_db=True)


class BaseImpSaleDoc(BaseImpSale):
    """
        Базовый класс импорта документа-чек
    """

    obj_id = None
    doc_id_cash = None
    doc_type_code = None
    external_obj_id = None
    doc_date = None
    doc_number = None
    doc_sum = None
    cash_type = None

    doc_sales_id = None
    doc_id = None
    result_save = True

    def save(self):
        """
            Сохранение документа-чек
            Сохранение чека должно происходить полностью
        """

        sql_text = 'select * from RBS_Q_SALE_DOCUMENT_SAVE(?,?,?,?,?,?,?,?)'
        sql_params = [self.obj_id, self.doc_id_cash, self.doc_type_code, self.external_obj_id,
                      TimeStampToDateTime(self.doc_date), self.doc_number, self.doc_sum, self.cash_type]
        res = self.p_c.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch='one',
                                   auto_commit=False)
        if res['status'] == c.kr_sql_error:
            ''' ошибка сохранения шапки чека '''
            self.p_c.log_file(c.m_e_cash_document_save % self.doc_number + c.kr_term_double_enter)
            ''' откатим транзакцию '''
            self.p_c.db.rollback()
            self.result_save = False
        else:
            self.doc_sales_id = res['datalist']['docsalesid']
            self.doc_id = res['datalist']['docid']

    def save_end(self):
        """
            commit или rollback транзакции, взависимости от результа
        """

        if self.result_save:
            self.p_c.db.commit()
        else:
            self.p_c.db.rollback()


class BaseImpSaleCargo(BaseImpSale, BaseImpSaleDoc):
    """
        Базовый класс импорта документа-позиция
    """

    wares_code = None
    amount = None
    price = None
    summa = None

    def save(self):
        """
            Сохранение документа-чек-
            Сохранение чека должно происходить полностью
        """

        sql_text = 'execute procedure RBS_Q_SALE_CARGO_SAVE(?,?,?,?,?)'
        sql_params = [self.doc_sales_id, self.wares_code, self.amount, self.price, self.summa]
        save_cargo = self.p_c.execute_sql(sql_text,
                                          sql_params=sql_params,
                                          fetch='none',
                                          auto_commit=False)
        if save_cargo['status'] == c.kr_sql_error:
            ''' ошибка сохранения позиции чека '''
            message = c.m_e_cash_cargo_save % (self.doc_id_cash, self.wares_code) + c.kr_term_double_enter
            self.p_c.log_file(message)
            self.result_save = False
