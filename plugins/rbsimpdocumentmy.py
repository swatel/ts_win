# -*- coding: utf-8 -*-
# swat 30.03.2012
# version 1.0.0.0
# модуль импорта документов из внешних систем для системы Мой Магазин


import os
import configparser

import krconst
import krconst as c
import BasePlugin as Bp


class Plugin(Bp.BasePlugin):

    str_to_file = ''

    def run(self):
        # проверка на существование файла конфинга и документа
        if not self.exists_file(self.resqueue['rulefilename']):
            return False
        
        xmlfile = self.ParseFileXML(self.filenames)
        if self.result['result'] == krconst.plugin_error:
            return False
        # в документе обязательно должен присутствовать тег typedoc
        # по нему бдем определять какую секцию конфига читать
        document = xmlfile.find('document')
        
        self.import_config = configparser.ConfigParser()
        self.import_config.read(self.resqueue['rulefilename'])
        self.typedoc = self.xml_get_value_by_attr(document, 'typedoc')
        # получим название процедуры для document
        self.procdocument = self.ConfigGetValByKey('procdocument')
        # получим название параметров
        self.paramsdocument = self.ConfigGetValByKey('paramsdocument')
        if self.result['result'] == krconst.plugin_error:
            return False
        # по названию параметров находим параметры для XML
        self.pdoc = []
        self.pdoc = self.GetValueParams(document, self.paramsdocument)
        # вызываем процедуру моздания документа, если параметры получены без ошибок
        if self.result['result'] == krconst.plugin_error:
            return False
        documentres = self.ExecuteSQL(self.procdocument, 
                                      sqlparams=self.pdoc,
                                      fetch='one',
                                      ExtVer=True,
                                      auto_commit=False)
        if documentres['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_i_external_file % self.filenames + c.t_double_enter)
            self.str_to_file = self.str_to_file + documentres['error_db'] + c.t_enter
            self.str_to_file = self.str_to_file + '================================='
            self.db.rollback()
            self.save_log_file_single()
        else:
            # получим название процедуры для cargo
            self.proccargo = self.ConfigGetValByKey('proccargo')
            # получим название параметров
            self.paramscargo = self.ConfigGetValByKey('paramscargo')
            if self.result['result'] == krconst.plugin_error:
                return False
            docid = documentres['datalist']['DOCID']
            nameprocafter = documentres['datalist']['NAMEPROC']
            sumwithnds = documentres['datalist']['OSUMWITHNDS']

            ''' Создадим связь задания и документа '''
            self.create_queue_bond(docid)

            cargo = document.find('cargo')
            for position in cargo:
                self.pcargo = []
                self.pcargo.append(docid)
                self.pcargo.append(nameprocafter)
                self.pcargo.append(sumwithnds)
                self.pcargo = self.pcargo + self.GetValueParams(position, self.paramscargo)
                wares = self.ExecuteSQL(self.proccargo, 
                                        sqlparams=self.pcargo,
                                        fetch='one',
                                        ExtVer=True,
                                        auto_commit=False)
                if wares['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_external_file % self.filenames + c.t_double_enter)
                    self.str_to_file = self.str_to_file + wares['error_db'] + c.t_enter
                    self.str_to_file = self.str_to_file + '================================='
            if self.result['result'] == krconst.plugin_error:
                self.save_log_file_single()
                self.db.rollback()
                return False
            if len(nameprocafter) == 1:
                upstatus = self.ExecuteSQL('execute procedure RBS_Q_MY_IMPDOCXML_UPSTATUS(?,?)', 
                                           sqlparams=[docid, '1'],
                                           fetch='one',
                                           ExtVer=True,
                                           auto_commit=False)
                if upstatus['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_external_file % self.filenames + c.t_double_enter)
                    self.str_to_file = self.str_to_file + upstatus['error_db'] + c.t_enter
                    self.str_to_file = self.str_to_file + '================================='
            else:
                procafterimport = self.ExecuteSQL('execute procedure ' + nameprocafter + '(?)', 
                                                  sqlparams=[docid],
                                                  fetch='one',
                                                  ExtVer=True,
                                                  auto_commit=False)
                if procafterimport['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_external_file % self.filenames + c.t_double_enter)
                    self.str_to_file = self.str_to_file + procafterimport['error_db'] + c.t_enter
                    self.str_to_file = self.str_to_file + '================================='
            if self.result['result'] == krconst.plugin_ok:
                self.db.commit()
            else:
                self.save_log_file_single()

    def ConfigGetValByKey(self, key):
        try:
            return self.import_config.get(self.typedoc, key)
        except:
            self.TracebackLog(krconst.m_e_setting_task % self.resqueue['rulefilename'])
            self.result['result'] = krconst.plugin_error
            return None

    def GetValueParams(self, objxml, pstr):
        # по названию параметров находим параметры для XML
        pvalue = []
        try:
            for rbskey in pstr.split(','):
                externalkey = self.ConfigGetValByKey(rbskey)
                if externalkey != 'None':
                    pvalue.append(self.xml_get_value_by_attr(objxml, externalkey))
                else:
                    pvalue.append(None)
        except:
            self.TracebackLog(krconst.kr_message_error_externalfile % self.filenames)
            self.result['result'] = krconst.plugin_error
        return pvalue

    def create_queue_bond(self, docid):
        """
            Связь документа и задания
        """
        if docid:
            sql_text = 'execute procedure RBS_QUEUEBOND_INSERT (?,?,?,?,?)'
            sql_params = [None, self.queueid, docid, None, 'I']

            res = self.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch='none',
                                   auto_commit=False)
            if res['status'] == krconst.kr_sql_error:
                self.str_to_file = self.str_to_file + res['error_db'] + c.t_enter
                self.str_to_file = self.str_to_file + '================================='
                self.TracebackLog('')

    def save_log_file_single(self):
        """
            Сохранение файла лога в директорию слоя
        """
        if self.str_to_file != '':
            log_file = os.path.basename(self.filenames)
            log_file = log_file.replace('.xml', '.log')
            dir_file = '/base/ftp_exchange/' + self.layer_code + '/in/'
            log_file_tmp = os.path.join(dir_file, 'tmp_' + log_file)
            log_file = os.path.join(dir_file, log_file)

            ''' сохраняем сначала во временный файл '''
            try:
                self.text_save_to_file(self.str_to_file, log_file_tmp)
            except:
                self.TracebackLog('Ошибка сохранения во временый файл')
                self.log_to_db('Ошибка сохранения во временый файл')

            ''' переименовываем временный файл в нормальный '''
            try:
                self.move_file(log_file_tmp, log_file)
            except:
                self.TracebackLog('Ошибка переименования файла')
                self.log_to_db('Ошибка переименования файла')
