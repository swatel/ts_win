# -*- coding: utf-8 -*-

"""
    модуль импорта данных из viki mini используя ORM
"""

import BasePlugin as Bp
import krconst as c

from orm.utils import vikiminireader
from orm.models.viki.CashOpen import CashOpen
from orm.models.viki.CashClose import CashClose
from orm.models.viki import CashIn as Ci
from orm.models.viki import CashOut as Co
from orm.models.viki.Sale import Sale
from orm.models.viki import Cargo as Cg
from orm.models.viki import Payment as Pa

__author__ = 'khmelevskiy'
VERSION = '1.0.0.1'
DATE_VERSION = '08.05.2016'


class Plugin(Bp.BasePlugin):
    """
        Класс импорта из viki mini
    """

    str_to_file = ''

    def _exit(self, message=None, result=c.plugin_error):
        self.result['result'] = result
        if message is not None:
            self.log_file(message)
        return False

    def run(self):
        """
            Импорт данных
        """
        queue_params = self.xml_get_all_params(self.queueparamsxml, as_dic=True)
        equipment_hash = None
        if 'equipment_hash' in queue_params:
            equipment_hash = queue_params['equipment_hash']
        elif 'equipmentid' in queue_params or 'sub_folder' in queue_params:
            if 'equipmentid' in queue_params:
                equipmentid = queue_params['equipmentid']
            else:
                equipmentid = queue_params['sub_folder']
            sql_text = 'select EQUIPMENTHASH as equipment_hash from EQUIPMENT where EQUIPMENTID=?'
            sql_params = [equipmentid]
            row = self.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch='one')['datalist']
            equipment_hash = row['equipment_hash']
        if equipment_hash is None:
            return self._exit(message='В параметрах задания не указан кассовый терминал.')
        with vikiminireader(self.filenames) as reader:
            models = reader.read()
        # Список моделей, для которых нужен Ид документа
        has_doc_id = (Cg.Cargo, Pa.Payment)
        # Список моделей, для которых нужена сессия
        has_session_id = (Ci.CashIn, Co.CashOut)
        if models is not None:
            session_id = None
            doc_id = None
            for model in models:
                # Разборки с сессией
                if session_id is None:
                    # Выясняем сессию кассы
                    params = {'equipment_hash': equipment_hash}
                    if isinstance(model, CashOpen):
                        # Если первая операция - открытие смены, то создаем сессию
                        params['flag'] = ''
                        if model.save(self.execute_sql, params=params):
                            session_id = model.session_id
                        else:
                            return self._exit(message='Ошибка при открытии смены.' + "\n" +
                                                      model.last_db_execute_error)
                    else:
                        # Иначе ищем созданную ранее
                        params['flag'] = 'F'
                        try:
                            cash_open = CashOpen()
                            # Все модели транзакций содержат одинаковые первые 7 полей в файле
                            cash_open.transact_date = model.transact_date
                            cash_open.cashier_num = model.cashier_num
                            cash_open.save(self.execute_sql, params=params)
                            session_id = cash_open.session_id
                        except Exception as e:
                            self.log_file(e.message)
                    if session_id is None:
                        return self._exit(message='Не удалось открыть сессию для кассы')
                else:
                    if isinstance(model, CashClose):
                        model.session_id = session_id
                        if model.save(self.execute_sql):
                            session_id = None
                        else:
                            return self._exit(message='Ошибка при закрытии смены.' + "\n" +
                                                      model.last_db_execute_error)
                if isinstance(model, has_session_id):
                    model.session_id = session_id
                    if not model.save(self.execute_sql):
                        return self._exit(message='Ошибка при проведении операции.' + "\n" +
                                                  model.last_db_execute_error)
                # Встретили документ
                if isinstance(model, Sale):
                    # Продажа
                    params = {'equipment_hash': equipment_hash}
                    if model.save(self.execute_sql, params=params, auto_commit=False):
                        doc_id = model.doc_id
                    else:
                        self.db.rollback()
                        return self._exit(message='Ошибка при создании чека.' + "\n" +
                                                  model.last_db_execute_error)
                else:
                    if isinstance(model, has_doc_id):
                        model.doc_id = doc_id
                        model.session_id = session_id
                        if not model.save(self.execute_sql):
                            self.db.rollback()
                            return self._exit(message='Ошибка при создании продажи/оплаты.' + "\n" +
                                                      model.last_db_execute_error)
                    else:
                        # Встретили операцию, которая не входит в документ
                        # Закроем транзацию
                        self.db.commit()
                        #  - сбросим doc_id
                        doc_id = None
