# -*- coding: utf-8 -*-

"""
    Общий модуль глобального экспорта
"""


__author__ = 'swat'
VERSION = '1.0.0.8'
DATE_VERSION = '24.06.2016'

import BasePlugin as Bp
import krconst as c


class Plugin(Bp.BasePlugin):
    """
        Класс глобального экспорта
        если:
            type_export = init то это инициализация экспорта, которая сгенерируется задания для экспорта
                иначе это конкретный экспорт
    """

    export_dic = None

    def run(self):
        """
        Экспорт
        """

        # получим параметры задания
        rule_dic = self.xml_get_all_params(self.queueparamsxml, as_dic=True)
        type_export = rule_dic['type_export']

        if type_export == 'init':
            self.init_export_taks()

    def init_export_taks(self):
        """
        Инициализация экспортных заданий по Q_EXCHANGE_TASK
        """

        pass
