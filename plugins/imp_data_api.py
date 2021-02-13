# -*- coding: utf-8 -*-

"""
    Модуль импорта с внещнимим системами через API
"""

import BasePlugin as Bp
import plugins.impdata.ecwid.imp_data_ecwid as sync_ecwid
import plugins.impdata.json.imp_data_json as imp_json

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '30.04.2015'


class Plugin(Bp.BasePlugin):
    """
        Класс импорта с внещнимим системами через API
    """

    def run(self):
        """
            Запуск плагина
        """

        a = sync_ecwid.ImpEcwid(self)
        json_data = a.convert_reference()[0]
        if json_data['type_data'] == 'data':
            data = imp_json.ImpDataJson(self, json_data['data'][0], encode=False)
            data.import_data()
        #a.sync_wgroup_ecwid_save_rbs()
        #a.sync_wgroup_ecwid_save_ecwid()
        #a.sync_wares_ecwid_save_rbs()
