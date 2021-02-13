# -*- coding: utf-8 -*-

"""
    импорт документов из json CommerceML
"""

import krconst as c
import plugins.impdocument.imp_doc_base_liteboxe as d
import plugins.impdocument.imp_cargo_base_litebox as cg
import plugins.impdata.imp_base as i_base

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '21.07.2015'


class ImpDocJsonCommerceML(i_base.ImpBase):
    """
        класс импорт документов из json CommerceML
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

    def import_document(self):
        """
            Импорт
        """

        for itm in self.json_data['document']:
            doc = d.BaseDocumentLB()
            doc.parent_class = self.parent_class
            self.obj = itm

            doc.type_doc = self.json_get_value('type_doc')
            doc.number_doc = self.json_get_value('number_doc')
            doc.date_doc = self.json_get_value('doc_date')
            doc.date_print = self.json_get_value('doc_date')

            doc.external_id = self.json_get_value('doc_id')
            doc.customer_number_doc = None
            doc.customer_date_doc = None
            doc.sum_with_nds = None
            doc.through_id = None
            doc.through_code = None
            doc.through_name = None
            doc.through_type = None

            for obj in itm['objects']:
                self.obj = obj
                if self.json_get_value('role') == 'Продавец':
                    doc.from_id = None
                    doc.from_code = self.json_get_value('giud')
                    doc.from_name = self.json_get_value('name')
                    doc.from_type = 'C'
                if self.json_get_value('role') == 'Покупатель':
                    doc.to_id = None
                    doc.to_code = self.json_get_value('giud')
                    doc.to_name = self.json_get_value('name')
                    doc.to_type = 'C'

            doc.save()
            if doc.doc_id:
                for pos in itm['cargo']:
                    self.obj = pos
                    cargo = cg.BaseCargoLB()
                    cargo.parent_class = self.parent_class
                    cargo.doc_id = doc.doc_id
                    cargo.action_status = doc.name_proc
                    cargo.sum_with_nds = doc.o_sum_with_nds
                    cargo.code_wares = self.json_get_value('code')
                    cargo.external_id = self.json_get_value('guid')
                    cargo.name_wares = self.json_get_value('name')
                    cargo.external_tax_code = self.json_get_value('tax_name')
                    cargo.code_unit = self.json_get_value('main_unit_code')

                    self.obj = pos['cargo_doc'][0]
                    cargo.amount = self.json_get_value('amount')
                    cargo.price = self.json_get_value('price')
                    cargo.doc_sum = self.json_get_value('summa')

                    cargo.save()
