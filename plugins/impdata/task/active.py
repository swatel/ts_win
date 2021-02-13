# -*- coding: utf-8 -*-

"""
    Импорт заданий для активности
"""

import krconst as c
import BasePlugin as Bp
from rbsqutils import decodeXStr

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '01.10.2015'


class Plugin(Bp.BasePlugin):
    """
        Класс заданий для активности
    """

    def run(self):
        """
            Запуск плагина
        """

        xml_file = self.parse_file_xml(self.filenames)
        if self.result['result'] == c.plugin_error:
            return False

        root = xml_file.getroot()

        #root = root.find(u'ФайлОбмена')

        shop_prefix = self.xml_attrib_value(root, 'ПрефиксМагазина')
        if shop_prefix:
            number_out = self.xml_attrib_value(root, 'НомерИсходящегоСообщения')
            number_in = self.xml_attrib_value(root, 'НомерВходящегоСообщения')

            sql_text = 'execute procedure glob_cua_number_loaded_set(?,?,?)'
            sql_params = [shop_prefix, number_out, number_in]
            res = self.execute_sql(sql_text,
                                   sql_params = sql_params,
                                   fetch='none',
                                   auto_commit=False)
            if res['status'] == c.kr_sql_error:
                message = 'Ошибка добавления шапки файла.'

                self.log_file(message,
                              terms=2,
                              save_log_db=True)

                # откатываем тразакцию
                self.db.rollback()
                return False

            for itm in root:
                period = self.xml_attrib_value(itm, 'Период')
                customer_code = self.xml_attrib_value(itm, 'КодПоставщика')
                shop_code = self.xml_attrib_value(itm, 'КодМагазина')
                wares_code = self.xml_attrib_value(itm, 'КодНоменклатуры')
                active = self.xml_attrib_value(itm, 'Активен')
                type_operation = self.xml_attrib_value(itm, 'ВидОперации')

                # сохраняем в БД все в одной транзакции
                sql_text = 'insert into GLOB_ASSORTMENT_ACTIVITY_CUA (WARESCODE, SUPPLCODE, SHOPCODE, FROMDATE,' \
                           '            ISACTIVE, ADDREMOVE, STATUS, NUMBERMESSAGE1C, NUMBERMESSAGERBS, PREFIXSHOP)' \
                           'values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                sql_params = [wares_code, customer_code, shop_code, period,
                              active, type_operation, '0', number_out, number_in, shop_prefix]

                res = self.execute_sql(sql_text,
                                       sql_params = sql_params,
                                       fetch='none',
                                       auto_commit=False)
                if res['status'] == c.kr_sql_error:
                    message = 'Ошибка добавления записи товара ' + wares_code

                    self.log_file(message,
                                  terms=2,
                                  save_log_db=True)

                    # откатываем тразакцию
                    self.db.rollback()
                    return False

            if res['status'] == c.plugin_ok:
                self.db.commit()
        else:
            shop_prefix = self.xml_value(root, 'ПрефиксМагазина')
            number_out = self.xml_value(root, 'НомерИсходящегоСообщения')
            number_in = self.xml_value(root, 'НомерВходящегоСообщения')
            shop_code = self.xml_value(root, 'КодМагазина')

            sql_text = 'execute procedure glob_cua_number_loaded_set(?,?,?)'
            sql_params = [shop_prefix, number_out, number_in]
            res = self.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch='none',
                                   auto_commit=False)
            if res['status'] == c.kr_sql_error:
                message = 'Ошибка добавления шапки файла.'

                self.log_file(message,
                              terms=2,
                              save_log_db=True)

                # откатываем тразакцию
                self.db.rollback()
                return False
            data_actives = root.find('ДанныеАктивности')
            if data_actives:
                for itm in data_actives.findall('Активность'):
                    period = self.xml_value(itm, 'Период')
                    customer_code = self.xml_value(itm, 'ПоставщикКод')
                    wares_code = self.xml_value(itm, 'НоменклатураКод')
                    active = self.xml_value(itm, 'Активен')
                    type_operation = self.xml_value(itm, 'ВидОперации')
                    if active == 'false':
                        active = '0'
                    else:
                        active = '1'

                    # сохраняем в БД все в одной транзакции
                    sql_text = 'insert into GLOB_ASSORTMENT_ACTIVITY_CUA (WARESCODE, SUPPLCODE, SHOPCODE, FROMDATE,' \
                               '            ISACTIVE, ADDREMOVE, STATUS, NUMBERMESSAGE1C, NUMBERMESSAGERBS, PREFIXSHOP)' \
                               'values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                    sql_params = [wares_code, customer_code, shop_code, period,
                                  active, type_operation, '0', number_out, number_in, shop_prefix]

                    res = self.execute_sql(sql_text,
                                           sql_params=sql_params,
                                           fetch='none',
                                           auto_commit=False)
                    if res['status'] == c.kr_sql_error:
                        message = 'Ошибка добавления записи товара ' + wares_code

                        self.log_file(message,
                                      terms=2,
                                      save_log_db=True)

                        # откатываем тразакцию
                        self.db.rollback()
                        return False

                data_status = root.find('СтатусыНоменклатуры')
                if data_status:
                    for itm in data_status.findall('Статус'):
                        wares_code = self.xml_value(itm, 'НоменклатураКод')
                        wares_type = self.xml_value(itm, 'Статус')

                        # сохраняем в БД все в одной транзакции
                        sql_text = 'update or insert into GLOB_ASSORTMENT_ACTIVITY_CUA (WARESCODE, SHOPCODE, NUMBERMESSAGE1C, NUMBERMESSAGERBS, PREFIXSHOP, WARESTYPE)' \
                                   'values (?, ?, ?, ?, ?, ?) ' \
                                   'matching (WARESCODE, SHOPCODE, NUMBERMESSAGE1C, NUMBERMESSAGERBS, PREFIXSHOP'
                        sql_params = [wares_code, shop_code, number_out, number_in, shop_prefix, wares_type]

                        res = self.execute_sql(sql_text,
                                               sql_params=sql_params,
                                               fetch='none',
                                               auto_commit=False)
                        if res['status'] == c.kr_sql_error:
                            message = 'Ошибка добавления записи товара статус' + wares_code

                            self.log_file(message,
                                          terms=2,
                                          save_log_db=True)

                            # откатываем тразакцию
                            self.db.rollback()
                            return False

                if res['status'] == c.plugin_ok:
                    self.db.commit()
            # Добавляем задание на экспорт
            # sql_text = 'execute procedure Q_GLOB_CREATE_QUEUE_ACTIVE(?)'
            # sql_params = [shop_prefix]
            # res = self.execute_sql(sql_text,
            #                       sql_params = sql_params,
            #                       fetch='none')
            # if res['status'] == c.kr_sql_error:
            #    message = 'Ошибка создания задания на экспорт'
            #    self.log_file(message,
            #                  terms=2,
            #                  save_log_db=True)

    @staticmethod
    def xml_attrib_value(xml, key):
        """
            Поиск значения по ключу
        """

        try:
            text = xml.attrib[key]
        except:
            text = None
        if text:
            text = text.encode('cp1251', 'ignore')
        return text

    def xml_value(xml, key, def_value=None):
        """
        Поиск значения по ключу
        :param xml: xml
        :param key: ключ
        :param def_value: значение по умолчанию
        :return: значение
        """

        try:
            text = xml.find(key).text
        except:
            text = None
        if text:
            text = text.encode('cp1251', 'ignore')
            text = text.replace('\t', '')
            text = text.replace('\n', '')
        else:
            text = def_value
        return text
