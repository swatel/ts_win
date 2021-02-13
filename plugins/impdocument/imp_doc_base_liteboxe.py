# -*- coding: utf-8 -*-

"""
    Базовый модуль импорта документов
"""

import krconst as c

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '21.07.2015'


class BaseDocumentLB(object):
    """
        Базовый класс импорта документа
    """

    parent_class = None

    type_doc = None
    number_doc = None
    date_doc = None
    date_print = None
    from_id = None
    from_code = None
    from_name = None
    from_type = None
    to_id = None
    to_code = None
    to_name = None
    to_type = None
    external_id = None
    customer_number_doc = None
    customer_date_doc = None
    sum_with_nds = None
    through_id = None
    through_code = None
    through_name = None
    through_type = None
    doc_id_input = None

    doc_id = None
    name_proc = None
    o_sum_with_nds = None

    def __init__(self):
        """
            Инициализация
        """

        pass

    def save(self):
        """
            сохраним документ
        """

        sql_text = 'select * from Q_IMP_DOC_LB(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [self.type_doc, self.number_doc, self.date_doc, self.date_print,
                      self.from_id, self.from_code, self.from_name, self.from_type,
                      self.to_id, self.to_code, self.to_name, self.to_type,
                      self.external_id, self.customer_number_doc, self.customer_date_doc,
                      self.sum_with_nds,
                      self.through_id, self.through_code, self.through_name, self.through_type,
                      self.doc_id_input]
        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='one')
        if res['status'] == c.kr_sql_error:
            message = c.m_w_i_doc_not_complete
            self.parent_class.LogFile(message + c.t_double_enter)
        else:
            try:
                self.doc_id = res['datalist']['DOCID']
                self.name_proc = res['datalist']['NAMEPROC']
                self.o_sum_with_nds = res['datalist']['OSUMWITHNDS']
            except:
                pass
        self.create_queue_bond()

    def create_queue_bond(self):
        """
            Связь документа и задания
        """

        if self.doc_id:
            sql_text = 'execute procedure RBS_QUEUEBOND_INSERT (?,?,?,?,?)'
            sql_params = [None, self. parent_class.queueid, self.doc_id, None, 'I']

            res = self.parent_class.execute_sql(sql_text,
                                                sql_params=sql_params,
                                                fetch='none')
            if res['status'] == c.kr_sql_error:
                self.parent_class.TracebackLog('')

    def update_status(self):
        """
            Поднятие статуса документа
        """

        if self.name_proc:
            if len(self.name_proc) == 1:
                sql_text = 'execute procedure Q_IMP_DOC_UPSTATUS(?,?)'
                up_status = self.parent_class.execute_sql(sql_text,
                                                          sql_params=[self.doc_id, '1'],
                                                          fetch='one')
            else:
                sql_text = 'execute procedure ' + self.name_proc + '(?)'
                up_status = self.parent_class.execute_sql(sql_text,
                                                          sql_params=[self.doc_id],
                                                          fetch='one')
            if up_status['status'] == c.kr_sql_error:
                self.log_file(c.m_e_i_external_file % self.parent_class.filenames,
                              terms=2)
