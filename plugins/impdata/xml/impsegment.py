# -*- coding: utf-8 -*-
"""
    swat 10.01.2014
    version 0.0.2.0
    класс импорта сегмента c привязкой групп товаров
"""

import krconst as k


class Segment():
    """
        класс импорта сегмента c привязкой групп товаров
    """

    parent_class = None

    segment_code = None
    segment_name = None
    group_gwares_code = None
    group_gwares_name = None

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self.parent_class = parent_class

        if obj is not None:
            self.segment_code = self.xml_get_value(obj, 'code')
            self.segment_name = self.xml_get_value(obj, 'name')
            self.group_gwares_code = self.xml_get_value(obj, 'groupgwarescode')
            self.group_gwares_name = self.xml_get_value(obj, 'groupgwares')

    def save(self):
        """
            Сохранение
        """

        sql_params = [self.segment_code, self.segment_name,
                      self.group_gwares_code, self.group_gwares_name]
        sql_text = 'execute procedure RBS_Q_SEGMENT_INS(?,?,?,?)'

        res = self.parent_class.ExecuteSQL(sql_text,
                                           sqlparams=sql_params,
                                           fetch='none',
                                           ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_segment % (self.segment_code, self.group_gwares_code)
            self.parent_class.log_file(message,
                                       terms=2,
                                       save_log_db=True)

    def xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self.parent_class.xml_get_value_by_attr(xml, attr, flag)