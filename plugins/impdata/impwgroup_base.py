# -*- coding: utf-8 -*-

"""
    swat 04.11.2014
    version 0.0.2.2
    Базовый модуль импорта групп товаров
"""

import krconst as c

VERSION = '0.0.2.2'


class BaseWGroup(object):
    """
        Базовый класс импорта групп товаров
    """

    # parent_class - ссылка на класс который унаследован от BasePlugin, что бы иметь доступ
    # к необходимым методам. Или класс в котором есть методы execute_sql и log_file
    parent_class = None

    name = None
    code = None
    parent_code = None
    delete_marker = None
    parent = None
    external_id = None
    # ид родителя
    group_id = None

    import_flag = 'I'

    wgroup_id = None
    wgroup_tree = []

    # дополнительные параметры для разделения групп
    singularity = None
    egaisneed = None

    def __init__(self):
        """
            Иницализация
        """

        pass

    def save(self):
        """
            сохраним группу
        """

        self.wgroup_id = None

        sql_text = 'select * from RBS_Q_WARESGROUP_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?)'
        sql_params = [self.name, self.code, self.parent_code, self.import_flag, self.delete_marker,
                      self.parent, self.external_id, self.group_id, None, None, None, None]

        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='one')
        if res['status'] == c.kr_sql_error:
            self.parent_class.log_file(c.m_e_i_wgroup % self.code + c.t_double_enter)
        else:
            self.wgroup_id = res['datalist']['WARESGRID']

    def get_tree(self):
        """
            Получение дерева групп товара
        """

        sql_text = 'select FIRST 10 t.*, wg.externalcode as hexternalcode, wg.externalid as hexternalid ' \
                   'from rbs_q_get_waresgroup_tree(?) t ' \
                   'left join waresgroup wg on t.higher = wg.waresgrid '
        sql_params = [None]

        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='all')
        if res['status'] == c.kr_sql_error:
            self.parent_class.log_file(c.m_e_g_wgroup_tree + c.t_double_enter)
        else:
            self.wgroup_tree = res['datalist']

    def update_external(self, waresgrid, external_code, external_id):
        """
            Обновление полей связи с внешними системами
        """

        sql_text = 'execute procedure RBS_Q_WARESGROUP_U_EXTERNAL(?,?,?)'
        sql_params = [waresgrid, external_code, external_id]

        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='none')
        if res['status'] == c.kr_sql_error:
            self.parent_class.log_file(c.m_e_u_wgroup_external % waresgrid + c.t_double_enter)

    def save_singularity(self):
        """
        Запись свойства группы товара
        """

        sql_text = 'execute procedure RBS_Q_WARESGROUP_SINGULARITY(?,?,?)'
        sql_params = [self.wgroup_id, self.singularity, self.egaisneed]

        res = self.parent_class.execute_sql(sql_text,
                                            sql_params=sql_params,
                                            fetch='none')
        if res['status'] == c.kr_sql_error:
            self.parent_class.log_file(c.m_e_u_wgroup_external % self.wgroup_id + c.t_double_enter)