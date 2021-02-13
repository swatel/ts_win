# -*- coding: utf-8 -*-
"""
    swat 14.02.2012
    модуль импорта документов из внешних систем
"""

import os
import configparser

import krconst
import BasePlugin as Bp

from rbsqutils import str_to_bool_int

VERSION = '0.0.0.1'


class Plugin(Bp.BasePlugin):
    """
        Класс импорта документов
    """

    import_config = None
    version_config = '0.0.0.0'
    import_with_task = False

    def run(self):
        """
            проверка на существование файла конфинга и документа
        """

        if not self.exists_file(self.resqueue['rulefilename'], add_log=True):
            return False

        xml_file = self.parse_file_xml(self.filenames)
        if self.result['result'] == krconst.plugin_error:
            return False

        ''' загрузим файл конфинурации импорта '''
        self.import_config = configparser.ConfigParser()
        self.import_config.read(self.resqueue['rulefilename'])

        ''' проверим версию файла конфигурации
            если версия не найдена, то необходимо в лог записать предупреждение
            и по умолчанию считать что это версия 0.0.0.0 '''
        try:
            self.version_config = self.import_config.get('VERSIONS', 'version')
        except:
            self.LogFile(krconst.m_w_i_conf_file_def_version, Terms=2)

        ''' проверим что является корнем файла xml
            если root то это стандартный документ
            иначе получаем специфический документ (остатки, продажи с кассы)
        '''
        if xml_file.getroot().tag == 'root':
            ''' в документе обязательно должен присутствовать тег typedoc
                по нему будем определять какую секцию конфига читать
            '''
            documents = xml_file.find('document')
            self.section = self.xml_get_value_by_attr(documents, 'typedoc')
            '''проверим есть ли в xml документе задания
                если есть то переходим в другую секцию
            '''
            if documents.find('task') is not None:
                self.section = self.xml_get_value_by_attr(documents.find('task'), 'typetask')
                self.import_with_task = True

        else:
            self.section = xml_file.getroot().tag
            documents = xml_file.find(self.section)

        ''' получим название процедуры для document '''
        self.procdocument = self.ConfigGetValByKey('procdocument')

        ''' получим название обязательных и не обязательных параметров
            и названия тегов, где они находятся в XML файлк
        '''
        self.paramsdocument = self.ConfigGetValByKey('paramsdocument')

        try:
            self.paramsdocumentconst = self.ConfigGetValByKey('paramsdocumentconst')
            if self.paramsdocumentconst == 'None':
                self.paramsdocumentconst = ''
        except:
            self.paramsdocumentconst = ''

        ''' получим порядок заполнения параметров для документа '''
        try:
            self.paramsdocumentoutside = self.ConfigGetValByKey('paramsdocumentoutside')
            if self.paramsdocumentoutside == 'None':
                self.paramsdocumentoutside = '0'
        except:
            self.paramsdocumentoutside = '0'

        ''' получим параметры которые нужно преобразовывать из строковых булевых в числовые '''
        try:
            self.convertparamsbool = self.ConfigGetValByKey('convertparamsbool')
            if self.convertparamsbool == 'None':
                self.convertparamsbool = ''
        except:
            self.convertparamsbool = ''

        ''' тег в котором находятся не изменяемые параметры шапки документа '''
        try:
            self.tagdocumentconst = self.ConfigGetValByKey('tagdocumentconst')
            if self.tagdocumentconst == 'None':
                self.tagdocumentconst = ''
        except:
            self.tagdocumentconst = ''

        ''' получим параметры, которые неоходимо брать из таблицы документ
            если версия 0.0.0.0 то для поддержки старого алгоритма будет равно DOCID,NAMEPROC,OSUMWITHNDS
        '''
        try:
            self.paramscargoconst = self.ConfigGetValByKey('paramscargoconst')
            if self.paramscargoconst == 'None':
                self.paramscargoconst = ''
        except:
            if self.version_config == '0.0.0.0':
                self.paramscargoconst = 'DOCID,NAMEPROC,OSUMWITHNDS'
            else:
                self.paramscargoconst = ''

        if self.result['result'] == krconst.plugin_error:
            return False

        '''  Параметр, который означает брать первый параметр, как название корня файла '''
        try:
            self.paramsdocumentfirstastag = self.ConfigGetValByKey('paramsdocumentfirstastag')
        except:
            self.paramsdocumentfirstastag = '0'

        ''' заполняем параметры которые не изменяются '''
        self.pdocinit = []
        self.pdocconst = []

        if self.paramsdocumentfirstastag == '1':
            if self.section in ('sales', 'objectsrest'):
                if self.section == 'sales':
                    self.pdocinit.append('SALE')
                if self.section == 'objectsrest':
                    self.pdocinit.append('CALCREM')
            else:
                self.pdocinit.append(self.section)

        if self.tagdocumentconst != '':
            self.pdocconst += (self.get_value_params(xml_file.getroot(), self.paramsdocumentconst))

        ''' получим название тега табличной части документа
            если версия 0.0.0.0 то для поддержки старого алгоритма будет равно cargo
        '''
        try:
            self.tagcargo = self.ConfigGetValByKey('tagcargo')
            if self.paramscargoconst == 'None':
                self.paramscargoconst = ''
        except:
            if self.version_config == '0.0.0.0':
                self.tagcargo = 'cargo'
            else:
                self.tagcargo = ''

        ''' для блока связанных документов '''
        try:
            self.tagdocbond = self.ConfigGetValByKey('tagdocbond', Exception=False)
        except:
            self.tagdocbond = None

        if self.tagdocbond is not None:
            # получим название процедуры для docbond
            self.procdocbond = self.ConfigGetValByKey('procdocbond')

            ''' получим название обязательных и не обязательных параметров
                и названия тегов, где они находятся в XML файлк
            '''
            self.paramsdocbondconst = self.ConfigGetValByKey('paramsdocbondconst')
            self.paramsdocbond = self.ConfigGetValByKey('paramsdocbond')


        # блок подчиненных записей под cargo
        try:
            self.tagcargobond = self.ConfigGetValByKey('tagcargobond', Exception=False)
        except:
            self.tagcargobond = None
        if self.tagcargobond is not None:
            # получим название процедуры для docbond
            self.proccargobond = self.ConfigGetValByKey('proccargobond')

            ''' получим название обязательных и не обязательных параметров
                и названия тегов, где они находятся в XML файлк
            '''
            self.paramscargobondconst = self.ConfigGetValByKey('paramscargobondconst')
            self.paramscargobond = self.ConfigGetValByKey('paramscargobond')

        try:
            self.queuebond_create = self.ConfigGetValByKey('queuebond_create', Exception=False)
        except:
            self.queuebond_create = 1

        documents = xml_file.getroot()
        for document in documents:
            if not self.delete_document(document):
                self.pdoc = []

                if self.paramsdocumentoutside == '0':
                    ''' Добавим не изменяемы параметры '''
                    self.pdoc += self.pdocinit + self.pdocconst

                    ''' по названию параметров находим параметры для XML '''
                    if self.paramsdocument != 'None':
                        self.pdoc += self.get_value_params(document, self.paramsdocument)

                if self.paramsdocumentoutside == '1':
                    self.pdoc += self.pdocinit + self.get_value_params(document,
                                                                       self.paramsdocument)
                    self.pdoc += self.pdocconst

                ''' вызываем процедуру создания документа, если параметры получены без ошибок '''
                if self.result['result'] == krconst.plugin_error:
                    return False
                docid = None

                ''' создаем документ '''
                document_res = self.ExecuteSQL(self.procdocument,
                                               sqlparams=self.pdoc,
                                               fetch='one',
                                               ExtVer=True)
                if document_res['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_external_file % self.filenames, Terms=2)
                    try:
                        docid = document_res['datalist']['DOCID']
                    except:
                        pass
                    ''' Создадим связь задания и документа '''

                    if self.queuebond_create == 1:
                        self.create_queue_bond(docid)
                    message_file = document_res['message']
                    message_file = self.get_message(message_file, 'SQL traceback', True)
                    # если переимпорт не позволяет сделать статус документа
                    # if 'exc_reimp_wrong_status'.upper() in document_res['message']:
                    #     message_file = self.get_message(message_file, 'EXC_REIMP_WRONG_STATUS',
                    #                                     False)
                    #     self.log_file('Создание документа, ошибка переимпорта.', terms=2)
                    #     document.attrib['status'] = 'Ошибка переимпорта'.decode('cp1251')
                    #     document.attrib['descript'] = message_file.decode('cp1251')
                    #     new_file_name = os.path.basename(self.filenames)
                    #     new_file_name = os.path.join('/base/share/temp/new', new_file_name)
                    #     xml_file.write(new_file_name, encoding='cp1251')
                else:
                    docid = document_res['datalist']['DOCID']
                    ''' проверим что возвращается, если = -1, то пропускаем  данный документ '''
                    if docid > 0:
                        ''' Создадим связь задания и документа '''
                        if self.queuebond_create == 1:
                            self.create_queue_bond(docid)

                        ''' получим название процедуры для cargo '''
                        self.proccargo = self.ConfigGetValByKey('proccargo')

                        ''' получим название параметров '''
                        self.paramscargo = self.ConfigGetValByKey('paramscargo')

                        if self.result['result'] == krconst.plugin_error:
                            return False

                        self.pcargoinit = []
                        if self.paramscargoconst:
                            for key in self.paramscargoconst.split(','):
                                self.pcargoinit.append(document_res['datalist'][key])

                        if document.tag != self.tagcargo:
                            cargo = document.find(self.tagcargo)
                        else:
                            cargo = document
                        if cargo:
                            for position in cargo:
                                self.pcargo = []
                                self.pcargo += self.pcargoinit
                                self.pcargo += self.get_value_params(position,
                                                                     self.paramscargo)
                                if self.import_with_task:
                                    self.pcargo.append('P')

                                wares = self.ExecuteSQL(self.proccargo,
                                                        sqlparams=self.pcargo,
                                                        fetch='one',
                                                        ExtVer=True)
                                if wares['status'] == krconst.kr_sql_error:
                                    self.LogFile(krconst.m_e_i_external_file % self.filenames, Terms=2)
                                else:

                                # если есть подчиненные к cargo записи
                                    if self.tagcargobond:
                                        try:
                                            cargobonds = position.find(self.tagcargobond)
                                        except:
                                            cargobonds = None
                                        if cargobonds:
                                            pcargobondinit = []
                                            if self.paramscargobondconst:
                                                for key in self.paramscargobondconst.split(','):
                                                    pcargobondinit.append(wares['datalist'][key])
                                            for cargobond in cargobonds:
                                                pcargobond = []
                                                pcargobond += pcargobondinit
                                                pcargobond += self.get_value_params(cargobond,
                                                                                    self.paramscargobond)
                                                cb_res = self.ExecuteSQL(self.proccargobond,
                                                                         sqlparams=pcargobond,
                                                                         fetch='one',
                                                                         ExtVer=True)
                                                if cb_res['status'] == krconst.kr_sql_error:
                                                    self.LogFile(krconst.m_e_i_external_file % self.filenames, Terms=2)

                        if self.result['result'] == krconst.plugin_error:
                            return False

                        if self.import_with_task:
                            cargo = document.find('task')
                            for position in cargo:
                                self.pcargo = []
                                self.pcargo += self.pcargoinit
                                self.pcargo += self.get_value_params(position,
                                                                     self.paramscargo)
                                if self.import_with_task:
                                    self.pcargo.append('F')

                                wares = self.ExecuteSQL(self.proccargo,
                                                        sqlparams=self.pcargo,
                                                        fetch='one',
                                                        ExtVer=True)
                                if wares['status'] == krconst.kr_sql_error:
                                    self.LogFile(krconst.m_e_i_external_file % self.filenames, Terms=2)
                            if self.result['result'] == krconst.plugin_error:
                                return False

                        ''' проверим есть ли связанные документы '''
                        if self.tagdocbond is not None:
                            docbonds = document.find(self.tagdocbond)
                            if docbonds is not None:
                                self.ppdocbondinit = []
                                for key in self.paramsdocbondconst.split(','):
                                    self.ppdocbondinit.append(document_res['datalist'][key])
                                for docbond in docbonds:
                                    self.pdocbond = []
                                    self.pdocbond += self.ppdocbondinit
                                    self.pdocbond = self.pdocbond + self.get_value_params(docbond, self.paramsdocbond)

                                    wares = self.ExecuteSQL(self.procdocbond,
                                                            sqlparams=self.pdocbond,
                                                            fetch='None',
                                                            ExtVer=True)
                                    if wares['status'] == krconst.kr_sql_error:
                                        self.LogFile(krconst.m_e_i_external_file % self.filenames, Terms=2)
                                if self.result['result'] == krconst.plugin_error:
                                    return False

                        name_proc_after = None
                        #if self.version_config == '0.0.0.0':
                        #   name_proc_after = documentres['datalist']['NAMEPROC']
                        #else:
                        #    name_proc_after = documentres['datalist']['ACTIONSTATUS']
                        try:
                            name_proc_after = document_res['datalist']['NAMEPROC']
                        except KeyError:
                            if self.section in ('sales', 'objectsrest'):
                                name_proc_after = 'RBS_Q_I_FILE_PREAORDER_NV'

                        is_delete = '0'
                        try:
                            is_delete = document_res['datalist']['is_delete']
                        except KeyError:
                            is_delete = '0'

                        if self.section not in ('sales', 'objectsrest'):
                            ''' для sales, objectsrest будем вызывать в конце для всех документов'''
                            if name_proc_after:
                                if len(name_proc_after) == 1:
                                    sql_text = 'execute procedure RBS_Q_IMPDOCXML_UPSTATUS(?,?)'
                                    up_status = self.execute_sql(sql_text,
                                                                 sql_params=[docid, '1'],
                                                                 fetch='one')
                                    if up_status['status'] == krconst.kr_sql_error:
                                        self.log_file(krconst.m_e_i_external_file % self.filenames,
                                                      terms=2)
                                else:
                                    if is_delete == '0':
                                        sql_text = 'execute procedure ' + name_proc_after + '(?)'
                                        proc_after_import = self.execute_sql(sql_text,
                                                                             sql_params=[docid],
                                                                             fetch='one')
                                        if proc_after_import['status'] == krconst.kr_sql_error:
                                            self.log_file(krconst.m_e_i_external_file % self.filenames,
                                                          terms=2)
                                            # если ошибка exc_wh_wrongamount то сделаем формирование задание на
                                            # экспорт во внешнюю систему
                                            # пока просто костыль, если понадобиться нужно сделать настройку в БД
                                            # сделано для U3S
                                            # message_file = proc_after_import['message']
                                            # message_file = self.get_message(message_file, 'SQL traceback',
                                            #                                 True)
                                            # if 'exc_wh_wrongamount'.upper() in proc_after_import['message'] or \
                                            #    'exc_wh_reserve'.upper() in proc_after_import['message']:
                                            #
                                            #     message_file = self.get_message(message_file, 'EXC_WH_WRONGAMOUNT',
                                            #                                     False)
                                            #     message_file = self.get_message(message_file, 'EXC_WH_RESERVE',
                                            #                                     False)
                                            #
                                            #     sql_text = 'execute procedure RBS_Q_CREATE_EXPORT_WRAMOUNT(?,?,?,?,?)'
                                            #     up_status = self.execute_sql(sql_text,
                                            #                                  sql_params=['auto', docid, None, None,
                                            #                                              message_file],
                                            #                                  fetch='one')
                                            #     if up_status['status'] == krconst.kr_sql_error:
                                            #         self.log_file(krconst.m_e_i_external_file % self.filenames,
                                            #                       terms=2)
                                            #         self.log_file('Ошибка создания докумета указывающее'
                                            #                       ' правильное кол-во.',
                                            #                       terms=2)
                                            # # Ошибка удаления документа
                                            # if 'EXC_WH_WRONGDOCSTAT' in proc_after_import['message']:
                                            #     message_file = self.get_message(message_file,
                                            #                                     'EXC_WH_WRONGDOCSTAT',
                                            #                                     False)
                                            #     if document.attrib['status'] == 'Удален':
                                            #         self.log_file('Создание документа, ошибка удаления документа.',
                                            #                       terms=2)
                                            #         document.attrib['status'] = 'Ошибка удаления'.decode(
                                            #             'cp1251')
                                            #     else:
                                            #         self.log_file('Создание документа, ошибка импорта документа.',
                                            #                       terms=2)
                                            #         document.attrib['status'] = 'Ошибка импорта'.decode(
                                            #             'cp1251')
                                            #     document.attrib['descript'] = message_file.decode('cp1251')
                                            #     new_file_name = os.path.basename(self.filenames)
                                            #     new_file_name = os.path.join('/base/share/temp/new',
                                            #                                  new_file_name)
                                            #     xml_file.write(new_file_name, encoding='cp1251')
                                    if is_delete == '1':
                                        sql_text = 'execute procedure ' + name_proc_after + '(?,?)'
                                        proc_after_import = self.execute_sql(sql_text,
                                                                             sql_params=[docid, 'D'],
                                                                             fetch='one')
                                        if proc_after_import['status'] == krconst.kr_sql_error:
                                            self.log_file(krconst.m_e_i_external_file % self.filenames,
                                                          terms=2)

                    else:
                        ''' пропускаем данный документ '''
                        self.log_file(krconst.m_w_i_doc_not_complete,
                                      terms=2,
                                      save_log_db=True)
                        try:
                            msg = document_res['datalist']['MESSAGESTR']
                            self.log_file(msg,
                                          terms=2,
                                          save_log_db=True)
                        except KeyError:
                            self.log_file(krconst.m_w_i_doc_not_complete_e_msg,
                                          terms=2,
                                          save_log_db=True)

        ''' Для АЗ: запустить процедуру которая перенесет дату+время из imp_document в document '''
        if self.section in ('sales', 'objectsrest'):
            if self.result['result'] != krconst.plugin_error:
                name_proc_after = 'RBS_Q_I_FILE_PREAORDER_NV'
                sql_text = 'execute procedure ' + name_proc_after + '(?)'
                proc_after_import = self.execute_sql(sql_text,
                                                     sql_params=[self.queueid],
                                                     fetch='one')
                if proc_after_import['status'] == krconst.kr_sql_error:
                    self.log_file(krconst.m_e_i_external_file % self.filenames,
                                  terms=2)
    @staticmethod
    def get_message(message, sub_message, start=True):
        """
        получение сообщения из исключения
        @param message: строка
        @param sub_message: подстрока
        @param start: если True то отрезать вначале строки, если False, То в конеце
        @return:
        """
        pos = message.find(sub_message)
        if pos > 0:
            if start:
                message = message[0: pos - 1]
            else:
                message = message[pos:]
        message = message.replace(sub_message, '')
        return message

    def ConfigGetValByKey(self, key, Exception=True):
        """
            Из конфига по ключу получим значение
        """

        key_value = None
        try:
            key_value = self.import_config.get(self.section, key)
        except:
            if Exception:
                self.TracebackLog(krconst.m_e_i_doc_conf_file_not_correct %
                                  (self.resqueue['rulefilename'], self.filenames),
                                  SaveLogDB=True)
                self.result['result'] = krconst.plugin_error
        return key_value

    def get_value_params(self, obj_xml, p_str):
        """
            по названию параметров находим параметры для XML
        """

        pvalue = []
        addSNYATIE = None
        try:
            for rbskey in p_str.split(','):
                externalkey = self.ConfigGetValByKey(rbskey)
                if externalkey != 'None':
                    if externalkey in self.convertparamsbool:
                        val = str_to_bool_int(self.xml_get_value_by_attr(obj_xml, externalkey))
                    else:
                        val = self.xml_get_value_by_attr(obj_xml, externalkey)
                    pvalue.append(val)
                    if rbskey == 'SNYATIE' and externalkey != 'None':
                        if val == 'Да' or val == '1':
                            docbonds = obj_xml.find('docbonds')
                            if docbonds is not None:
                                for docbond in docbonds:
                                    if externalkey in self.convertparamsbool:
                                        val = str_to_bool_int(self.xml_get_value_by_attr(docbond, 'id1c'))
                                    else:
                                        val = self.xml_get_value_by_attr(docbond, 'id1c')
                                    pvalue.append(val)
                                    addSNYATIE = 1
                            else:
                                pvalue.append(None)
                                addSNYATIE = 1
                        if val == 'Нет' or val == '0':
                            pvalue.append(None)
                            addSNYATIE = 1
                else:
                    if rbskey == 'EXTERNALIDNEW':
                        if addSNYATIE is None:
                            pvalue.append(None)
                    else:
                        ''' если есть параметр QUEUEID то автоматом добавим значение '''
                        if rbskey == 'QUEUEID':
                            pvalue.append(self.queueid)
                        else:
                            pvalue.append(None)
        except:
            self.TracebackLog(krconst.kr_message_error_externalfile % self.filenames)
            self.result['result'] = krconst.plugin_error
        return pvalue

    def delete_document(self, document):
        """
            проверка на удаление. И в случае необходимости удаление документа
        """

        try:
            delete_status_value = self.import_config.get('DELETE', 'statusvalue')
            status_name = self.import_config.get('DELETE', 'status')
            external_id_name = self.import_config.get('DELETE', 'externalid')
            status_del_proc = self.import_config.get('DELETE', 'statusproc')
        except:
            # todo сделать вывод в лог
            ''' удаление не поддерживается '''
            return False
        try:
            status = self.xml_get_value_by_attr(document, status_name)
            external_id = self.xml_get_value_by_attr(document, external_id_name)
        except:
            return False
        if status != delete_status_value:
            ''' документ не в статусе на удаление '''
            return False
        else:
            if status_del_proc and status_del_proc != 'None':
                # sql_text = 'execute procedure RBS_Q_IMPDOCXML_DEL_NV_WH (?)'
                sql_text = 'execute procedure %s (?)' % status_del_proc
                sql_params = [external_id]
                res = self.execute_sql(sql_text,
                                       sql_params=sql_params,
                                       fetch='none')
                if res['status'] == krconst.kr_sql_error:
                    self.TracebackLog('')
                return True
            else:
                ''' пробуем найти документ, если его нет, то нет смысла импортировать '''
                sql_text = 'select * from RBS_Q_DOC_FIND_EXTID(?)'
                sql_params = [external_id]

                res = self.execute_sql(sql_text,
                                       sql_params=sql_params,
                                       fetch='one')
                if res['status'] == krconst.kr_sql_error:
                    self.TracebackLog('')
                else:
                    if len(res['datalist']) == 0:
                        return True
                    else:
                        return False
