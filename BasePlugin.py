# -*- coding: utf-8 -*-

import os
import re
import xml.etree.ElementTree as Et
from datetime import datetime

import krconst
import krconst as c
import BasePRLogger as Blog
import kconfig as kc
import queue_db as db
import rbsqutils as rqu

from utils.decorator import timer_sql

try:
    import kmssql as ms
except ImportError:
    ms = None

try:
    import kmysql as my
except ImportError:
    my = None

try:
    import kpostgresql as postgre
except ImportError:
    postgre = None

VERSION = '0.0.2.8'


class BasePlugin(object):
    """
        Базовый класс плагинов
    """

    def __init__(self, *args, **kwargs):

        self.parent = self.get_in_params(args, 'parent')
        self.task = self.get_in_params(args, 'taskparams')

        self.taskparamsxml = None
        self.taskactionsparamsxml = None
        self.logfilename = None
        self.delsuccessfiles = None
        self.logrenewnum = None
        self.logextended = None
        self.quetaskid = None
        self.tasktype = None

        if self.task:
            self.taskparamsxml = self.task['taskparams']
            self.taskactionsparamsxml = self.task['taskactionsparams']
            self.logfilename = self.task['logfilename'].replace('\\', '/')
            self.delsuccessfiles = self.task['delsuccessfiles']
            self.logrenewnum = self.task['logrenewnum']
            self.logextended = self.task['logextended']
            self.quetaskid = self.task['quetaskid']
            self.tasktype = self.task['tasktype']
            self.layer_code = self.task['layer_code']
            self.sn_name = self.task['sn_name']

        self.db = self.get_in_params(args, 'db')
        self.queueid = self.get_in_params(args, 'queueid')

        self.resqueue = self.get_in_params(args, 'resqueue')

        self.queueparamsxml = None
        self.queueactionsparamsxml = None
        self.rule = None
        self.filenames = None
        self.ruleparams = None
        self.ruleactionsparams = None

        if self.resqueue:
            self.queueparamsxml = self.resqueue['queueparams']
            self.queueactionsparamsxml = self.resqueue['queueactionsparams']
            self.rule = self.resqueue['rule']
            self.filenames = self.resqueue['filename']
            self.ruleparams = self.resqueue['ruleparams']
            self.ruleactionsparams = self.resqueue['ruleactionsparams']

        self.result = dict()
        self.result['result'] = krconst.plugin_ok
        self.result['log'] = ''
        self.result['logDB'] = ''

        self.result['Restart'] = False
        self.result['LostConnect'] = False

        self.command_text = None
        self.datestart = datetime.today()
        # configure log plugin
        self.base_plugin_log = None
        if self.logfilename != '':
            self.base_plugin_log = Blog.RPLogger(cfg=self.parent.k_conf,
                                                 path=self.logfilename,
                                                 file_size=self.logrenewnum)
        self.LogFile('-' * 5)
        if self.queueid:
            self.LogFile('Start queueid = ' + str(self.queueid))

        ''' проверка параметров на валидность данных '''

        if self.taskparamsxml:
            self.xml_check_valid_string(self.taskparamsxml)

        if self.taskactionsparamsxml:
            self.xml_check_valid_string(self.taskactionsparamsxml)

        if self.queueparamsxml:
            self.xml_check_valid_string(self.queueparamsxml)

        if self.queueactionsparamsxml:
            self.xml_check_valid_string(self.queueactionsparamsxml)

        if self.ruleparams:
            self.xml_check_valid_string(self.ruleparams)

        if self.ruleactionsparams:
            self.xml_check_valid_string(self.ruleactionsparams)

        self.xml_convert = None

        ''' Для экспорта '''
        self.export_file_name = None

        #self.execute_sql = timer_sql(self)(self.execute_sql)

    def get_in_params(self, args, name):
        """
            Получение входящих параметров
        """

        try:
            return args[0][name]
        except KeyError:
            return None

    def run(self):
        if self.command_text:
            self.ExecuteSQL(self.command_text)

    def EndRunPlugin(self):
        if self.result:
            # add parse actionsparams
            pass
        type_log = None
        status = None
        if self.result['result'] == krconst.plugin_ok:
            type_log = krconst.log_info
            status = krconst.kr_status_ok
            if self.delsuccessfiles == '1' \
                    and (self.filenames is not None and self.filenames != ''):
                if not self.delete_tmp_file(self.filenames, delete_dir=True):
                    self.LogFile(krconst.m_e_delete_file % self.filenames)
                    self.result['result'] = krconst.plugin_error
        if self.result['result'] == krconst.plugin_error:
            type_log = krconst.log_error
            status = krconst.kr_status_error
        if self.result['Restart']:
            type_log = krconst.log_warning
            status = krconst.kr_status_new
            self.result['result'] = krconst.plugin_restart
        if status:
            self.update_status_turn_db(self.queueid, status)

        #self.create_task_email()

        # проверим тип задания и необходимость создания репорта для типа заданий импорт
        if self.tasktype == 'I':
            if self.ParserXML(self.taskparamsxml, 'CreateImportReport'):
                sql_text = 'select* from RBS_Q_CREATETASKEXPORT_REPORT(?,?,?,?)'
                if not self.xml_convert:
                    ext_params = None
                else:
                    ext_params = '<to value="%s"/>' \
                                 '<from value="%s"/>' \
                                 '<outsms value="%s"/>' \
                                 '<insms value="%s"/>' % \
                                    (self.xml_convert['from'],
                                     self.xml_convert['to'],
                                     self.xml_convert['outsms'],
                                     self.xml_convert['insms'])
                sql_params = ['TASKSERVER', self.queueid, None, ext_params]
                resm = self.ExecuteSQL(sql_text,
                                       sqlparams=sql_params,
                                       fetch='many',
                                       ExtVer=True)
                if resm['status'] == krconst.kr_sql_error:
                    self.LogFile("Ошибка создания отчета об импорте" + krconst.kr_term_double_enter)

        'save log file'
        self.LogFile('Runtime = ' + str(datetime.today() - self.datestart))
        if type_log:
            self.SaveLogPlugin(self.result['log'], type_log)
        return True

    ''' работа с xml '''
    def xml_check_valid_string(self, params_xml):
        """
            Проверка XML строки на валидность
        """

        xml = None
        try:
            # xml = Et.fromstring(params_xml.decode("cp1251").encode("utf-8"))
            xml = Et.fromstring(params_xml)
        except:
            self.LogFile(krconst.m_e_xml_parse_str % params_xml, Terms=1, SaveLogDB=True)
            self.result['result'] = krconst.plugin_error
        return xml

    def ParserXML(self, paramsxml, key=None):
        #todo удалить после рефакторинга
        return self.parser_xml(paramsxml, key)

    def parser_xml(self, param_xml, key=None):
        """
            Получение параметра из XML
        """

        xml = self.xml_check_valid_string(param_xml)
        if xml is None:
            return None
        if not key:
            self.LogFile('Exec ParserXML without key')
        else:
            ''' get value from key '''
            try:
                key = xml.find(key).attrib['value']#.encode("cp1251")
                if key == '':
                    key = None
            except:
                key = None
            return key

    def parse_file_xml(self, file_name):
        """
            Парсинг XML файла
        """

        if not self.exists_file(file_name, add_log=True):
            return None

        xml_file = None
        try:
            xml_file = Et.parse(file_name)
        except:
            message = krconst.m_e_xml_parse_error % file_name
            self.log_file(message,
                          terms=1,
                          save_log_db=True)
            self.result['result'] = krconst.plugin_error
        return xml_file

    def ParseFileXML(self, filenames):
        """
            Парсинг файла
        """
        #todo  удалить после рефакторинга
        return self.parse_file_xml(filenames)

    def xml_get_all_params(self, params_xml, as_dic=None):
        """
            Возвращает значение ключей xml строки
        """

        xml = self.xml_check_valid_string(params_xml)
        if xml is None:
            return None
        params = []
        dic = {}
        for i in xml.getiterator():
            if list(i.items()):
                if as_dic is None:
                    params.append({i.tag: list(i.items())[0][1]})
                else:
                    dic[i.tag] = list(i.items())[0][1]
        if as_dic is None:
            return params
        else:
            return dic

    def XMLGetAllParams(self, paramsxml, asdic=None):
        #todo удалить после рефакторинга
        return self.xml_get_all_params(paramsxml, asdic)

    def xml_get_all_params_from_file(self, file_xml, as_dic=None):
        """
            Возвращает значение ключей xml файла
        """

        params = []
        dic = {}
        for i in file_xml.getiterator():
            if list(i.items()):
                if as_dic is None:
                    params.append({i.tag: list(i.items())[0][1]})
                else:
                    dic[i.tag] = list(i.items())[0][1]
        if as_dic is None:
            return params
        else:
            return dic

    def xml_get_value_by_attr(self, xml, attr, flag='E'):
        """
            Значение параметра по атрибуту
        """

        if not attr:
            self.log_file('Exec xml_get_value_by_attr without attr')
        val = xml.get(attr)

        ''' проверка на пустую дату которая может прийти из 1С '''
        if val == '01.01.0001 0:00:00':
            val = None
        if val == '':
            val = None
        if val is not None:
            # return val.encode("cp1251", 'ignore')
            return val
        else:
            if flag == 'E':
                return ''
            if flag == 'N':
                return None
    # работа с xml

    @timer_sql
    def execute_sql(self, sql_text, sql_params=(), auto_commit=True, db_local=None, fetch='many', ext_ver=True):
        """
            выполнение SQL комманд
            с выводом в лог файл информации при возникновении deadlock
        """

        if not self.result['LostConnect']:
            res = []
            try:
                if not db_local:
                    db_local = self.db

                res = db_local.dbExec(sql_text,
                                      params=sql_params,
                                      fetch=fetch,
                                      auto_commit=auto_commit)

                if db_local.db_message.find('Unable to complete network request to host') != -1:
                    self.result['result'] = krconst.plugin_error
                    self.result['LostConnect'] = True
                    self.log_to_db(db_local.db_message)
                    self.log_file(db_local.db_message)
                    if ext_ver:
                        return {'status': krconst.kr_sql_lost_connect, 'message': db_local.db_message, 'datalist': None}
                    else:
                        return [krconst.kr_sql_lost_connect, db_local.db_message, None]
                else:
                    if ext_ver:
                        return {'status': krconst.kr_sql_ok, 'message': None, 'datalist': res}
                    else:
                        return [krconst.kr_sql_ok, None, res]
            except Exception as exc:
                self.result['result'] = krconst.plugin_error
                error = 'Error execute SQL command: %(sql)s %(sqlparams)s, %(err)s'\
                        % {'sql': sql_text, 'sqlparams': sql_params, 'err': exc[1]}
                error_db = exc[1]
                self.log_to_db(error)
                self.log_file(error)

                ''' Проверим на ошибку. Если  deadlock, то найдем чем он заблокирован '''
                if re.findall('deadlock|lock conflict', exc[1]):
                    try:
                        ''' найдем id транзакции '''
                        p = re.compile(r'concurrent transaction number is (\d+)')
                        transact_id = p.search(exc[1]).group(1)
                        sql_text = 'select * from RBS_Q_DEADLOCK_GET_PROCESS(?)'
                        sql_params = [transact_id]
                        res = db_local.dbExec(sql_text,
                                              params=sql_params,
                                              fetch='one',
                                              auto_commit=auto_commit)
                        attachment_id = res['ATTACHMENT_ID']
                        remote_address = res['REMOTE_ADDRESS']
                        remote_process = res['REMOTE_PROCESS']

                        error_deadlock = 'REMOTE ADDRESS: ' + remote_address + krconst.kr_term_enter + ''\
                                         'REMOTE PROCESS: ' + remote_process + krconst.kr_term_enter + ''\
                                         'SQL:' + krconst.kr_term_enter

                        sql_text = 'select * from RBS_Q_DEADLOCK_GET_SQL(?,?)'
                        sql_params = [transact_id, attachment_id]
                        res = db_local.dbExec(sql_text,
                                              params=sql_params,
                                              fetch='many',
                                              auto_commit=auto_commit)

                        for itm in res:
                            error_deadlock = error_deadlock + '     ' + itm['SQL_TEXT'] + krconst.kr_term_enter
                            error_deadlock = error_deadlock + '==============================' + krconst.kr_term_enter
                    except:
                        error_deadlock = 'Ошибка получения данных по deadlock'

                    error_deadlock = krconst.kr_term_enter + error_deadlock
                    self.log_to_db(error_deadlock)
                    self.LogFile(error_deadlock)

                if ext_ver:
                    return {'status': krconst.kr_sql_error, 'message': error, 'datalist': None, 'error_db': error_db}
                else:
                    return [krconst.kr_sql_error, error, None]
        else:
            self.result['result'] = krconst.plugin_error
            error = 'Execute SQL command with lost connections. Unable to complete network request to host.'
            self.log_to_db(error)
            self.LogFile(error)
            if ext_ver:
                return {'status': krconst.kr_sql_lost_connect, 'message': error, 'datalist': None, 'error_db': error}
            else:
                return [krconst.kr_sql_lost_connect, error, None]

    def ExecuteSQL(self, sqltext, sqlparams=(), auto_commit=True, db_local=None, fetch='many', ExtVer=False):
        #todo после перехода удалить

        return self.execute_sql(sqltext,
                                sql_params=sqlparams,
                                auto_commit=auto_commit,
                                db_local=db_local,
                                fetch=fetch,
                                ext_ver=ExtVer)

    def log_file(self, message, terms=0, save_log_db=False):
        """
            Логирование в файл
        """

        self.result['log'] += rqu.decodeXStr(message) + krconst.kr_term_space
        while terms > 0:
            self.result['log'] += krconst.kr_term_enter
            terms -= 1
        if save_log_db:
            self.log_to_db(message, terms=terms)

    def LogFile(self, message, Terms=0, SaveLogDB=False):
        """
            Логирование в файл
        """

        self.log_file(message, Terms, SaveLogDB)

    def log_to_db(self, message, terms=0):
        """
            Логирование в БД
        """

        self.result['logDB'] += rqu.decodeXStr(message) + krconst.kr_term_enter
        while terms > 0:
            self.result['logDB'] += krconst.kr_term_enter
            terms -= 1

    def SaveLogPlugin(self, message, typemessage='INFO'):
        # write log
        if self.base_plugin_log:
            self.base_plugin_log.write(message, krconst.kr_flag_logplugin, typemessage)

    def update_status_turn_db(self, queueid, status, params=None):
        """
            Обновление статуса задания
        """

        if queueid:
            log = None
            if self.result['logDB'] != '':
                log = self.result['logDB']

            sql_text = 'update r_queue r set r.status = ? '
            if log:
                sql_text += ', r.result = B_PUT_SEGMENT(?) '
            else:
                sql_text += ', r.result = ? '
            sql_params = [status, log]
            if status == krconst.kr_status_new:
                sql_text += ' , r.starttime = DATEADD(10 MINUTE to r.starttime) '
            if self.export_file_name:
                sql_text += ' , r.resultfilename = ?, r.resultfilesize = ?'
                sql_params += [self.export_file_name, os.path.getsize(self.export_file_name)]
            if params:
                sql_text += ', r.params = ? '
                sql_params += [params]
            sql_text += ' where r.queueid = ? '
            sql_params += [queueid]
            try:
                res = self.db.dbExec(sql_text,
                                     params=sql_params,
                                     fetch='None')
                return [krconst.kr_sql_ok, None]
            except Exception as exc:
                self.LogFile('Error update status queueid:')
                error = 'Error execute SQL command: %(sql)s %(sqlparams)s, %(err)s' % \
                        {'sql': sql_text, 'sqlparams': sql_params, 'err': exc[1]}
                self.log_to_db(error)
                self.log_file(error)
                return [krconst.kr_sql_error, error]

    def UpdateResult(self, result):
        if result == krconst.plugin_restart:
            self.result['Restart'] = True
            self.result['result'] = krconst.plugin_restart
        else:
            if result == krconst.plugin_error:
                self.result['result'] = krconst.plugin_error
            else:
                self.result['result'] = krconst.plugin_error
                error = 'Not support actions ' + result + '.'
                self.log_to_db(error)
                self.LogFile(error)

    def export_post_job(self, filename, actionsparams):
        if os.access(filename, os.F_OK):
            xml = self.XMLGetAllParams(actionsparams, True)
            for item in xml:
                pass
        else:
            self.LogFile("Cannot find file " + filename)

    def check_file_in_queue_sort(self, filename, flag, que_sort_id=None, file_name_dest=None):
        """
        Проверка файла в сортировщике
        @param filename: имя файла
        @param flag: флаг, откуда пришел запрос на формирование задания
        @param que_sort_id: ид сортировки
        @param file_name_dest: куда скопировать файл после обработки
        @return: ид задания
        """

        res = None
        if flag == 'CheckDir':
            try:
                sql_text = 'select R.RULE, R.QUETASKID from RBS_Q_QUEUESORT (?,?) R'
                sql_params = [self.parent.q_server_id, os.path.basename(filename)]
                res = self.db.dbExec(sql_text,
                                     params=sql_params,
                                     fetch='many')
            except Exception as exc:
                self.log_file(exc[1])
        else:
            res = 1

        if res:
            file_size = None
            try:
                file_size = os.path.getsize(filename)
            except Exception as exc:
                self.LogFile(exc[1])

            # определяем тип создания задания
            if flag == 'CheckDir':
                sql_text = 'select R.QUEUEID from RBS_Q_CREATEQUEUE_FILENAME (?,?,?,?,?,?,?) R'
                sql_params = [res[0]['QUETASKID'], res[0]['RULE'], flag, os.path.basename(filename), 10,
                              self.parent.k_conf.global_def_dir_tmp_files, file_size]
            else:
                sql_text = 'select R.QUEUEID from Q_CREATEQUEUE_FILENAME_CONVERT (?,?,?,?,?,?,?) R'
                sql_params = [que_sort_id, flag, os.path.basename(filename), 10,
                              self.parent.k_conf.global_def_dir_tmp_files, file_size,
                              file_name_dest]

            # создаем задание с статусом L, что бы скопировать файл в нужную папку и обновить статус заданию в 0
            try:
                cr_queue = self.db.dbExec(sql_text,
                                          params=sql_params,
                                          fetch='many')
                return cr_queue[0]['QUEUEID']
            except Exception as exc:
                self.LogFile(exc[1])
                return None
        else:
            return None

    def is_exists_folder(self, path):
        """
            Проверка на существование каталога
        """

        return os.path.exists(os.path.dirname(path) + '/')

    def create_folder(self, path):
        """
            Создание каталога
        """

        if not self.is_exists_folder(path):
            os.makedirs(os.path.dirname(path))

    def copy_file(self, src_file, dst_file):
        """
            Копирование файла
        """

        self.create_folder(dst_file)
        import shutil
        try:
            shutil.copy(src_file, dst_file)
        except:
            self.LogFile(krconst.kr_message_error_errorcopyfile % (src_file, dst_file))
            self.result['result'] = krconst.plugin_error
            return False
        return True

    def move_file(self, src_file, dst_file):
        """
            Перенименование файла
        """

        self.create_folder(dst_file)
        import shutil
        try:
            shutil.move(src_file, dst_file)
        except:
            self.log_file(c.kr_message_error_errormovefile % (src_file, dst_file), terms=1)
            self.result['result'] = krconst.plugin_error
            return False
        return True

    def delete_tmp_file(self, full_file_name, delete_dir=False):
        """
        Удаление файла + директории если нужно, если она пуста
        @param full_file_name: имя файла
        @param delete_dir: признак удаления директории если она пуста
        @return: успешность действие
        """
        if self.exists_file(full_file_name, add_log=False):
            try:
                os.unlink(full_file_name)
            except:
                raise
                return False
        if delete_dir:
            ''' проверим есть ли в каталоге еще файлы'''
            file_dir = os.path.dirname(full_file_name)
            try:
                if not os.listdir(file_dir):
                    import shutil
                    shutil.rmtree(file_dir)
            except:
                raise
                return False

        return True

    def delete_dir(self, dir_name):
        """
            Удаление каталога
        """

        import shutil
        try:
            shutil.rmtree(dir_name)
        except:
            raise
            return False
        return True

    def exists_file(self, file_name, add_log=True):
        """
            Существование файла, с записью в лог при необходимости
        """

        if not os.access(file_name, os.F_OK):
            if add_log:
                self.result['result'] = krconst.plugin_error
                self.log_file(krconst.m_e_file_not_found % file_name,
                              save_log_db=True)
            return False
        else:
            return True

    def text_save_to_file(self, text, dst_file):
        """
            Сохранение текста в файл
        """

        self.create_folder(dst_file)
        if os.access(dst_file, os.F_OK):
            self.delete_tmp_file(dst_file)
        file_save = open(dst_file, "a")
        print(text, file=file_save)
        file_save.close()

    def read_config_other_db(self, name_db):
        """
            Чтение настроек для подключения к другой БД
        """

        other_db_cfg = kc.KConfig(name_db)
        if other_db_cfg:
            other_db_cfg.get_config_file()
            other_db_cfg.get_config()
            return other_db_cfg
        else:
            self.log_file(krconst.m_e_odb_name_none % name_db)
            return None

    def connect_other_db(self, other_cfg):
        other_db = db.QueryDB(other_cfg)
        if other_db.connect:
            return other_db
        else:
            if other_db.db_message != '':
                self.log_file(other_db.db_message)
                self.result['result'] = krconst.plugin_error
            return None

    def MSSQLConnect(self, name_db):
        MSSQLcfg = ms.MssqlConfig(name_db)
        if MSSQLcfg.status_config == krconst.kr_status_config_ok:
            MSSQLDB = ms.MssqlDb(MSSQLcfg)
            if MSSQLDB.db_connect:
                return MSSQLDB
            else:
                if MSSQLDB.db_message != '':
                    self.LogFile(MSSQLDB.db_message)
                    self.result['result'] = krconst.plugin_error
                return None
        else:
            self.LogFile(MSSQLcfg.status_config_message)
            self.result['result'] = krconst.plugin_error
            return None

    def MSSQLExecuteSQL(self, sqltext, sqlparams=(), auto_commit=True, db_local=None, fetch='many'):
        if db_local == None:
            self.result['result'] = krconst.plugin_error
            return {'status': krconst.kr_sql_error, 'message': krconst.kr_message_error_error_MSSQL_DBNone, 'datalist': None}
        else:
            if not self.result['LostConnect']:
                res = []
                try:
                    res = db_local.odb_exec(sqltext, params=sqlparams,
                                            fetch=fetch,
                                            auto_commit=auto_commit)
                    if db_local.db_message != '':
                        self.result['result'] = krconst.plugin_error
                        self.log_to_db(db_local.db_message)
                        self.LogFile(db_local.db_message)
                        return {'status': krconst.kr_sql_lost_connect, 'message': db_local.db_message, 'datalist': None}
                    else:
                        return {'status': krconst.kr_sql_ok, 'message': None, 'datalist': res}
                except Exception as exc:
                    self.result['result'] = krconst.plugin_error
                    error = 'Error execute SQL in MSSQL command: %(sql)s %(sqlparams)s, %(err)s' % {'sql': sqltext, 'sqlparams': sqlparams, 'err': db_local.db_message}
                    self.log_to_db(error)
                    self.LogFile(error)
                    return {'status': krconst.kr_sql_error, 'message': error, 'datalist': None}
            else:
                self.result['result'] = krconst.plugin_error
                error = 'Execute SQL command with lost connections. Unable to complete network request to host.'
                self.log_to_db(error)
                self.LogFile(error)
                return {'status': krconst.kr_sql_lost_connect, 'message': error, 'datalist': None}

    def PostgreSQLConnect(self, name_db):
        PostgreSQLcfg = postgre.PostgresqlConfig(name_db)
        if PostgreSQLcfg.status_config == krconst.kr_status_config_ok:
            PostgreSQLDB = postgre.PostgresqlDb(PostgreSQLcfg)
            if PostgreSQLDB.db_connect:
                return PostgreSQLDB
            else:
                if PostgreSQLDB.db_message != '':
                    self.LogFile(PostgreSQLDB.db_message)
                    self.result['result'] = krconst.plugin_error
                return None
        else:
            self.LogFile(PostgreSQLcfg.status_config_message)
            self.result['result'] = krconst.plugin_error
            return None

    def TracebackLog(self, message, SaveLogDB=False):
        message = rqu.TracebackLog(message)
        self.LogFile(message, SaveLogDB=SaveLogDB)

    ''' Работа с MySQL '''
    def mysql_connect(self, name_db):
        """
            Подключение к БД MySQL
        """
        mysql_cfg = my.MysqlConfig(name_db)
        if mysql_cfg.status_config == krconst.kr_status_config_ok:
            mysql_db = my.MysqlDb(mysql_cfg)
            if mysql_db.db_connect:
                return mysql_db
            else:
                if mysql_db.db_message != '':
                    self.LogFile(mysql_db.db_message)
                    self.result['result'] = krconst.plugin_error
                return None
        else:
            self.LogFile(mysql_cfg.status_config_message)
            self.result['result'] = krconst.plugin_error
            return None

    @timer_sql
    def odb_exec_sql(self, sql_text, sql_params=(), auto_commit=True, db_local=None, fetch='many'):
        """
            Выполнение запросов к MSSQL и MySQL
        """

        if not db_local:
            self.result['result'] = krconst.plugin_error
            return {'status': krconst.kr_sql_error, 'message': krconst.m_e_odb_db_none, 'datalist': None}
        else:
            if not self.result['LostConnect']:
                try:
                    res = db_local.odb_exec(sql_text,
                                            params=sql_params,
                                            fetch=fetch,
                                            auto_commit=auto_commit)
                    if db_local.db_message != '':
                        self.result['result'] = krconst.plugin_error
                        self.log_to_db(db_local.db_message)
                        self.log_file(db_local.db_message)
                        error = krconst.m_e_odb_exec_sql % {'db': db_local.dn_name,
                                                            'sql': sql_text,
                                                            'sql_params': sql_params,
                                                            'err': db_local.db_message}
                        self.log_to_db(error)
                        self.log_file(error)
                        if db_local.db_message.find('Unable to complete network request to host') != -1:
                            return {'status': krconst.kr_sql_lost_connect,
                                    'message': db_local.db_message,
                                    'datalist': None}
                        else:
                            return {'status': krconst.kr_sql_error,
                                    'message': db_local.db_message,
                                    'datalist': None}
                    else:
                        return {'status': krconst.kr_sql_ok, 'message': None, 'datalist': res}
                except:
                    self.result['result'] = krconst.plugin_error
                    error = krconst.m_e_odb_exec_sql % {'db': db_local.dn_name,
                                                        'sql': sql_text,
                                                        'sql_params': sql_params,
                                                        'err': db_local.db_message}
                    self.log_to_db(error)
                    self.log_file(error)
                    return {'status': krconst.kr_sql_error,
                            'message': error,
                            'datalist': None}
            else:
                self.result['result'] = krconst.plugin_error
                error = krconst.m_e_odb_lost_connect % db_local.dn_name
                self.log_to_db(error)
                self.log_file(error)
                return {'status': krconst.kr_sql_lost_connect,
                        'message': error,
                        'datalist': None}

    ''' Методы для работы с сетевыми устройствами '''
    def mount_dir(self):
        """
            Подключение ресурса: директории
        """

        mount_cmd = None
        un_mount_cmd = None
        try:
            mount_dir = self.ParserXML(self.taskparamsxml, 'MountDir')
            mount_cmd = self.ParserXML(self.taskparamsxml, 'MountCMD')
            un_mount_cmd = self.ParserXML(self.taskparamsxml, 'UMountCMD')
        except:
            mount_dir = '0'

        if mount_dir == '1':
            try:
                os.system(un_mount_cmd)
            except:
                self.log_file(krconst.m_e_mount_dir % un_mount_cmd)

            try:
                os.system(mount_cmd)
            except:
                self.log_file(krconst.m_e_mount_dir % mount_cmd)

    # def create_task_email(self):
    #     """
    #     Создание задания на отправку почты при необходимости
    #     """
    #
    #     # создаем задание если
    #     # 1. Задание завершилось с ошибкой
    #     # 2. Нужно отправлять письмо
    #
    #     if self.result['result'] == krconst.plugin_error \
    #             and self.parser_xml(self.taskparamsxml, 'send_error_email') == '1':
    #         sql_text = 'select * from RBS_Q_CREATEMAIL(?,?,?)'
    #         sql_params = ['', self.result['log'], '']
    #         res = self.execute_sql(sql_text,
    #                                sql_params = sql_params,
    #                                fetch='one')
    #         if res['status'] == krconst.kr_sql_error:
    #             self.log_file(krconst.m_e_createmail,
    #                           terms=2,
    #                           save_log_db=True)
    #         else:
    #             self.log_file('Создано задание на отправку почты.',
    #                           terms=2,
    #                           save_log_db=True)
    def create_task_email(self):
        """
        Создание задания на отправку почты при необходимости
        """

        # создаем задание если
        # 1. Задание завершилось с ошибкой
        # 2. Нужно отправлять письмо

        if self.result['result'] == krconst.kr_result_pligin_error:
            send_error_email = self.ParserXML(self.taskparamsxml, 'send_error_email')
            if send_error_email == '1':
                sql_text = 'select * from RBS_Q_CREATEMAIL(?,?,?)'
                sql_params = ['', self.result['log'], '']
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_createmail,
                                 Terms=2,
                                 SaveLogDB=True)
                else:
                    self.LogFile('Создано задание на отправку почты.',
                                 Terms=2,
                                 SaveLogDB=True)

    def create_queue_bond(self, docid, bond_type='I', auto_commit=True):
        """
        Связь документа и задания
        @param docid: Документ
        @param bond_type: Тип связи
        @param auto_commit: Автозавершение транзакции
        @return:
        """
        if docid is not None and self.queueid is not None:
            sql_text = 'execute procedure RBS_QUEUEBOND_INSERT (?,?,?,?,?)'
            sql_params = [None, self.queueid, docid, None, bond_type]

            res = self.execute_sql(sql_text, sql_params=sql_params, fetch='none', auto_commit=auto_commit)
            if res['status'] == krconst.kr_sql_error:
                # self.str_to_file = self.str_to_file + res['error_db'] + c.t_enter
                # self.str_to_file = self.str_to_file + '=================================' + c.t_enter
                self.TracebackLog('')
            else:
                return True
        return False
