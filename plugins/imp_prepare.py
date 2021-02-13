# -*- coding: utf-8 -*-

"""
    модуль предимпорта данных
    Определяет необходимость конвертации данных
"""

import json

import BasePlugin as Bp
import plugins.commerceml.commerceml as cml
import plugins.impdata.json.imp_data_json as imp_json
import plugins.impdocument.json_commercml.imp_doc as imp_doc

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '29.07.2015'


class Plugin(Bp.BasePlugin):
    """
        Класс предимпорта данных
    """

    def run(self):
        """
            Запуск плагина
        """

        f = cml.CommerceML(self, self.filenames)
        json_file = f.xml_parse()

        with open(json_file, 'r') as f:
            entry = json.load(f, encoding='cp1251')
        json_data = entry[0]
        # спровочники
        if json_data['type_data'] == 'data':
            data = imp_json.ImpDataJson(self, json_data['data'][0])
            data.import_data()
        # документы
        if json_data['type_data'] == 'document':
            doc = imp_doc.ImpDocJsonCommerceML(self, json_data['data'][0])
            doc.import_document()
