# -*- coding: utf-8 -*-

"""
    модуль импорта данных из json
    справочник
"""

import plugins.impdata.json.gwares.imp_wgroup_json as imp_wgroup
import plugins.impdata.json.gwares.imp_gwares_json as imp_gwares
import plugins.impdata.json.gwares.imp_barcode_json as imp_barcode

__author__ = 'swat'


class ImpDataJson(object):
    """
        Класс импорта данных из json
    """

    parent_class = None
    json_data = None
    encode = None

    def __init__(self, parent_class, json_data, encode=True):
        """
            Инициализация переменных
        """

        self.parent_class = parent_class
        self.json_data = json_data
        self.encode = encode

    def import_data(self):
        """
            Импорт
        """

        # импорт товарных групп
        try:
            json_catalog = self.json_data['catalog']
        except KeyError:
            json_catalog = None
        if json_catalog:
            for obj in json_catalog:
                wgroup = imp_wgroup.WGroupJson(self.parent_class, obj, self.encode)
                wgroup.save()
        try:
            json_gwares = self.json_data['gwares']
        except KeyError:
            json_gwares = None
        if json_gwares:
            for obj in json_gwares:
                gwares = imp_gwares.GwaresJson(self.parent_class, obj, self.encode)
                gwares.save()
                barcode = imp_barcode.BarcodeJson(self.parent_class, obj, self.encode)
                barcode.save()
