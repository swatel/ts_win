# -*- coding: utf-8 -*-
"""
    swat 20.05.2015
    version 0.0.3.0
    модуль выполения sql запросов
"""

import re

import krconst as c
import BasePlugin as Bp

VERSION = '0.0.3.0'


class Plugin(Bp.BasePlugin):
    """
        Класс выполения sql запросов
    """

    def run(self):
        """
            Запуск плагина
        """
        if self.queueparamsxml:
            self.command_exec(self.queueparamsxml)
        if self.taskparamsxml:
            self.command_exec(self.taskparamsxml)

    def command_exec(self, xml_text):
        """
            Выполнение SQL комманды
        """

        command_text = self.parser_xml(xml_text, 'CommandText')
        restart_deadlock = self.parser_xml(xml_text, 'restart_deadlock')

        ''' получим код БД, если нужен экспорт из другой БД '''
        odb = None
        db_code = self.parser_xml(xml_text, 'DBCODE')
        if db_code:
            odb_cfg = self.read_config_other_db(db_code)
            if not odb_cfg:
                self.log_file('Нет настроек доп БД!')
                self.result['result'] = c.plugin_error
                return False
            else:
                odb = self.connect_other_db(odb_cfg)
                if not odb:
                    self.log_file('Нет подключения к доп БД!')
                    self.result['result'] = c.plugin_error
                    return False

        if command_text:
            res = self.execute_sql(command_text,
                                   db_local=odb)
            if res['status'] == c.kr_sql_error:
                self.LogFile(res['message'])
            if res['status'] == c.kr_sql_error:
                if re.findall('deadlock|lock conflict', res['message']) \
                        and restart_deadlock == '1':
                    self.LogFile('Restart queue')
                    self.UpdateResult(c.plugin_restart)
