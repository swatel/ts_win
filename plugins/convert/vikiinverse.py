# -*- coding: utf-8 -*-

"""
    Модуль конвертации vikimini формата (обратная загрузка) в json стандарт LiteBox
"""

import os

import json
import time
import datetime

import BasePlugin as Bp
from rbsqutils import decodeUStr
from orm.models.viki.VikiReader import *
from orm.models.schema.general_viki_v0 import *
from orm.models.Waresgroup import Waresgroup
from orm.models.Gwares import Gwares


class Plugin(Bp.BasePlugin):
    groups = None

    def run(self):
        """
        Конвертация файла
        @return: Bool
        """

        file_name_dest = self.parser_xml(self.queueparamsxml, 'FileNameDest').replace('\\', '/')
        filename = self.filenames

        # проверим что находится в файле
        bases = {
            'ADDQUANTITY:WG': (Waresgroup, VikiWaresGroup),
        }
        self.groups = {}
        with context_viki_reader(filename, bases) as reader:
            for group in reader.read():
                self.groups[group.code] = group
        # Построение иерархии
        for code in self.groups:
            group = self.groups[code]
            if group.level is None:
                self.check_level(group)

        bases = {
            'ADDQUANTITY:GW': (Gwares, VikiGwares)
        }
        with context_viki_reader(filename, bases) as reader:
            gwares = []
            for wares in reader.read():
                group = None
                # Делаем все, что можно в модели
                if wares.wg_code is not None and wares.wg_code in self.groups:
                    group = self.groups[wares.wg_code]
                    wares.wg_name = group.name
                    wares.wg_level = group.level
                # Выгружаем в JSON и дополняем
                wares_json = wares.get_json()
                # Восстанавливаем иерархию
                if group.higher_code is not None and group.higher_code in self.groups:
                    wares_json['parent_wgroup'] = []
                    wg_higher = self.groups[group.higher_code]
                    while wg_higher is not None:
                        wares_json['parent_wgroup'].append(wg_higher.get_json())
                        wg_higher = self.groups[wg_higher.higher_code] \
                            if wg_higher.higher_code is not None and wg_higher.higher_code in self.groups else None
                gwares.append(wares_json)
            result_json = list()
            result_json.append({'gwares': gwares})
            json_result = decodeUStr(json.dumps(result_json, encoding='cp1251', indent=1).encode('cp1251'))

            ''' сохраняем в файл '''
            file_name_exp = os.path.join(file_name_dest, '%sgwares_%s.json')
            now = datetime.datetime.now()
            s2 = time.strftime('%Y%m%d%H%M%S', time.localtime()) + str(now.microsecond)
            try:
                file_name_exp = (file_name_exp % ('', s2)).replace('\\', '/')
            except:
                pass
            try:
                self.text_save_to_file(json_result, file_name_exp)
            except:
                self.TracebackLog('Ошибка сохранения во временый файл')
                self.log_to_db('Ошибка сохранения во временый файл')
        return True

    def check_level(self, group):
        if group.higher_code is None:
            # Корень
            group.level = 1
        elif group.higher_code in self.groups:
            higher = self.groups[group.higher_code]
            self.check_level(higher, self.groups)
            group.level = higher.level + 1
        else:
            raise ValueError('Не найдена родительская группа (код группы %s, код родительской группы %s)' %
                             (group.code, group.higher_code))
