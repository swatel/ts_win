# -*- coding: utf-8 -*-

import kconfig as conf
import queue_db as db
import BasePlugin as Bp

import plugins.ext.kb.clientbank as kb


class Plugin(Bp.BasePlugin):
    """

    """

    def run(self):
        params = {}
        if self.taskparamsxml:
            tmp_dir = self.parser_xml(self.taskparamsxml, 'check_dir').replace('\\', '/')
            params['sort_dir'] = tmp_dir.decode('cp1251')
            tmp_dir = self.parser_xml(self.taskparamsxml, 'out_dir').replace('\\', '/')
            params['out_dir'] = tmp_dir.decode('cp1251')
            tmp_dir = self.parser_xml(self.taskparamsxml, 'error_dir').replace('\\', '/')
            params['error_dir'] = tmp_dir.decode('cp1251')
            params['mask_files'] = self.parser_xml(self.taskparamsxml, 'mask_files')

            # получим подключение к БД Engine
            # todo Щеглов Сделать возможность хранить настроки не в коде
            # todo Сделать сначала сканирование, а затем подключение к Engine
            self.log_file('Подклчение к Engine', terms=1)
            engine_conf = conf.KConfig('ENGINE_LITEBOX')
            engine_conf.get_config_file()
            engine_conf.get_config_layer()
            engine_conf.get_config()
            db_engine = db.QueryDB(engine_conf)
            # todo Щеглов Сделать логирование
            if db_engine.connect:
                self.log_file('Подклчение к Engine прошло успешно', terms=1)
                bc = kb.BankClient(params)
                bc.process_import(self, engine_conf=engine_conf, db_engine=db_engine)
            else:
                self.log_file('Ошибка подклчения к Engine', terms=1)

