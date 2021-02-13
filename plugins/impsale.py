# -*- coding: utf-8 -*-
"""
    swat 27.11.2014
    version 0.0.2.2
    Основной модуль импорта чеков
"""

import krconst as c

import BasePlugin as Bp
import plugins.sale.mysql.impsale_ukm as _ukm
import plugins.sale.mssql.impsale_ses as _ses
import plugins.sale.postgres.impsale_ses as _ses10


class Plugin(Bp.BasePlugin):
    """
        Класс импорта чеков
    """

    def run(self):
        """
            Запуск плагина
        """

        ''' Определим из каких касс нужно импортировать чеки '''
        sql_text = 'select * from RBS_Q_GET_CASH'
        sql_params = []
        cash_import = self.execute_sql(sql_text,
                                       sql_params = sql_params,
                                       fetch='many')
        if cash_import['status'] == c.kr_sql_error:
            self.log_file(c.m_e_cash_get_cash + c.t_double_enter)
        else:
            ''' Для  соответствующий касс запускаем импорт'''
            for cash in cash_import['datalist']:
                i_ukm = None
                i_ses = None
                i_ses10 = None

                ''' Импорт из УКМ '''
                if cash['cash_code'] == 'UKM':
                    i_ukm = _ukm.ImportUKM(self)

                ''' Импорт из Кристалла '''
                if cash['cash_code'] == 'SES':
                    i_ses = _ses.ImportSES(self)

                ''' Импорт из Кристалла 10 '''
                if cash['cash_code'] == 'SES10':
                    i_ses10 = _ses10.ImportSES(self)

                if i_ukm:
                    if i_ukm.dict_doc_id:
                        for key in i_ukm.dict_doc_id:
                            self.save_end(key['doc_id_sale'], key['doc_id_ret'])
                if i_ses:
                    if i_ses.dict_doc_id:
                        for key in i_ses.dict_doc_id:
                            self.save_end(key['doc_id_sale'], key['doc_id_ret'])
                if i_ses10:
                    if i_ses10.dict_doc_id:
                        for key in i_ses10.dict_doc_id:
                            self.save_end(key['doc_id_sale'], key['doc_id_ret'])
                self.db.commit()

    def save_end(self, doc_id_sale, doc_id_ret):
        """
            обновим реализацию на основе чеков
        """

        sql_text = 'select * from RBS_Q_SALE_QUEUE(?,?)'
        sql_params = [doc_id_sale, doc_id_ret]

        res = self.execute_sql(sql_text,
                               sql_params=sql_params,
                               fetch='one',
                               auto_commit=False)
        if res['status'] == c.kr_sql_error:
            self.log_file(c.m_e_cash_status + c.t_double_enter)
        else:
            self.log_file(res['datalist']['msg'],
                          terms=1,
                          save_log_db=True)
