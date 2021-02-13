# -*- coding: utf-8 -*-

"""
    Импорт остатков
"""

import krconst as c
import BasePlugin as Bp
from rbsqutils import decodeXStr

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '23.11.2016'


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

        number = root.find('{http://www.samberi.com/hms/ExchangeRemains}Обмен')

        shop_prefix = self.xml_value(number, '{http://www.samberi.com/hms/ExchangeRemains}ПрефиксМагазина')
        number_out = self.xml_value(number, '{http://www.samberi.com/hms/ExchangeRemains}НомерИсходящегоСообщения')
        number_in = self.xml_value(number, '{http://www.samberi.com/hms/ExchangeRemains}НомерВходящегоСообщения')

        sql_text = 'execute procedure GLOB_HMS_NUMBER_LOADED_SET(?,?,?)'
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

        rest = root.find('Remains')
        for itm in rest:
            shop_code = self.xml_value(itm, '{http://www.samberi.com/hms/ExchangeRemains}Магазин')
            wares_code = self.xml_value(itm, '{http://www.samberi.com/hms/ExchangeRemains}Номенклатура')
            wares_rest = self.xml_value(itm, '{http://www.samberi.com/hms/ExchangeRemains}Остаток')

            # сохраняем в БД все в одной транзакции
            sql_text = 'insert into BLO0_HMS_GWARES_REST (WARESCODE, WARESREST, NUMBERHMS, NUMBERRBS)' \
                       'values (?, ?, ?, ?)'
            sql_params = [wares_code, wares_rest, number_out, number_in]

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

    def xml_value(self, xml, key, def_value=None):
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
