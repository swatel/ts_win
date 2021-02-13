# -*- coding: utf-8 -*-

"""
    Импорт транзитных графиков
"""

import krconst as c
import BasePlugin as Bp

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '09.12.2015'


class Plugin(Bp.BasePlugin):
    """
        Класс транзитных графиков
    """

    def run(self):
        """
            Запуск плагина
        """

        xml_file = self.parse_file_xml(self.filenames)
        if self.result['result'] == c.plugin_error:
            return False

        schedules = xml_file.find('schedules')
        if schedules is not None:
            for schedule in schedules:
                suppliercode = self.xml_get_value_by_attr(schedule, 'suppliercode')
                suppliername = self.xml_get_value_by_attr(schedule, 'suppliername')
                orderdate1 = self.xml_get_value_by_attr(schedule, 'orderdate1', flag='N')
                docdate1 = self.xml_get_value_by_attr(schedule, 'docdate1', flag='N')
                orderdate2 = self.xml_get_value_by_attr(schedule, 'orderdate2', flag='N')
                docdate2 = self.xml_get_value_by_attr(schedule, 'docdate2', flag='N')
                objects = schedule.find('objects')
                for obj in objects:
                    code = self.xml_get_value_by_attr(obj, 'code')

                    sql_text = 'execute procedure RBS_TRANSIT_SCHEDULES_INS(?,?,?,?,?,?,?)'
                    sql_params = [orderdate1, docdate1, orderdate2, docdate2, suppliercode,
                                  suppliername, code]
                    res = self.execute_sql(sql_text,
                                           sql_params= sql_params,
                                           fetch='none')
                    if res['status'] == c.kr_sql_error:
                        self.LogFile('Ошибка импорта транзитного графика')
