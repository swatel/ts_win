# -*- coding: windows-1251 -*-
# coding=cp1251

import os
import re
import glob
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

import threading
threadlocal = threading.local()


class BasePlugin(object):
    """
        ������� ����� ��������
    """

    log_flush = False

    def __init__(self, *args, **kwargs):

        self.parent = self.get_in_params(args, 'parent')
        self.task = self.get_in_params(args, 'taskparams')
        self.params_ext = self.get_in_params(args, 'params_ext')

        self.taskparamsxml = None
        self.taskactionsparamsxml = None
        self.logfilename = None
        self.delsuccessfiles = None
        self.logrenewnum = None
        self.logextended = None
        self.quetaskid = None
        self.tasktype = None
        self.layer_code = None
        self.sn_name = None

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

        if self.params_ext:
            self.log_flush = self.params_ext['log_flush']

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

        ''' �������� ���������� �� ���������� ������ '''

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

        ''' ��� �������� '''
        self.export_file_name = None

        # update turn db time (minute)
        self.update_turn_db_time = 10

        #self.execute_sql = timer_sql(self)(self.execute_sql)

    def get_in_params(self, args, name):
        """
            ��������� �������� ����������
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

        # �������� ��� ������� � ������������� �������� ������� ��� ���� ������� ������
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
                    self.LogFile("������ �������� ������ �� �������" + krconst.kr_term_double_enter)

        'save log file'
        self.LogFile('Runtime = ' + str(datetime.today() - self.datestart))
        if type_log:
            self.save_log_plugin(self.result['log'], type_log)
        return True

    ''' ������ � xml '''
    def xml_check_valid_string(self, params_xml):
        """
            �������� XML ������ �� ����������
        """

        xml = None
        try:
            xml = Et.fromstring(params_xml.decode("cp1251").encode("utf-8"))
        except:
            self.LogFile(krconst.m_e_xml_parse_str % params_xml, Terms=1, SaveLogDB=True)
            self.result['result'] = krconst.plugin_error
        return xml

    def ParserXML(self, paramsxml, key=None):
        #todo ������� ����� ������������
        return self.parser_xml(paramsxml, key)

    def parser_xml(self, param_xml, key=None):
        """
            ��������� ��������� �� XML
        """

        xml = self.xml_check_valid_string(param_xml)
        if xml is None:
            return None
        if not key:
            self.LogFile('Exec ParserXML without key')
        else:
            ''' get value from key '''
            try:
                key = xml.find(key).attrib['value'].encode("cp1251")
                if key == '':
                    key = None
            except:
                key = None
            return key

    def parse_file_xml(self, file_name):
        """
            ������� XML �����
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
            ������� �����
        """
        #todo  ������� ����� ������������
        return self.parse_file_xml(filenames)

    def xml_get_all_params(self, params_xml, as_dic=None):
        """
            ���������� �������� ������ xml ������
        """

        xml = self.xml_check_valid_string(params_xml)
        if xml is None:
            return None
        params = []
        dic = {}
        for i in xml.getiterator():
            if i.items():
                if as_dic is None:
                    params.append({i.tag.encode("cp1251"): i.items()[0][1].encode("cp1251")})
                else:
                    dic[i.tag.encode("cp1251")] = i.items()[0][1].encode("cp1251")
        if as_dic is None:
            return params
        else:
            return dic

    def XMLGetAllParams(self, paramsxml, asdic=None):
        #todo ������� ����� ������������
        return self.xml_get_all_params(paramsxml, asdic)

    def xml_get_all_params_from_file(self, file_xml, as_dic=None):
        """
            ���������� �������� ������ xml �����
        """

        params = []
        dic = {}
        for i in file_xml.getiterator():
            if i.items():
                if as_dic is None:
                    params.append({i.tag.encode("cp1251"): i.items()[0][1].encode("cp1251")})
                else:
                    dic[i.tag.encode("cp1251")] = i.items()[0][1].encode("cp1251")
        if as_dic is None:
            return params
        else:
            return dic

    def xml_get_value_by_attr(self, xml, attr, flag='E'):
        """
            �������� ��������� �� ��������
        """

        if not attr:
            self.log_file('Exec xml_get_value_by_attr without attr')
        val = xml.get(attr.decode("cp1251"))

        ''' �������� �� ������ ���� ������� ����� ������ �� 1� '''
        if val == '01.01.0001 0:00:00':
            val = None
        if val == '':
            val = None
        if val is not None:
            return val.encode("cp1251", 'ignore')
        else:
            if flag == 'E':
                return ''
            if flag == 'N':
                return None
    # ������ � xml

    @timer_sql
    def execute_sql(self, sql_text, sql_params=(), auto_commit=True, db_local=None, fetch='many', ext_ver=True):
        """
            ���������� SQL �������
            � ������� � ��� ���� ���������� ��� ������������� deadlock
        """

        if not self.result['LostConnect']:
            res = []
            try:
                if not db_local and self.db:
                    db_local = self.db

                if not db_local and not self.db:
                    db_local = getattr(threadlocal, 'resource_db', None)

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
            except Exception, exc:
                self.result['result'] = krconst.plugin_error
                error = 'Error execute SQL command: %(sql)s %(sqlparams)s, %(err)s'\
                        % {'sql': sql_text, 'sqlparams': sql_params, 'err': exc[1]}
                error_db = exc[1]
                self.log_to_db(error)
                self.log_file(error)

                ''' �������� �� ������. ����  deadlock, �� ������ ��� �� ������������ '''
                if re.findall('deadlock|lock conflict', exc[1]):
                    try:
                        ''' ������ id ���������� '''
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
                        error_deadlock = '������ ��������� ������ �� deadlock'

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
        #todo ����� �������� �������

        return self.execute_sql(sqltext,
                                sql_params=sqlparams,
                                auto_commit=auto_commit,
                                db_local=db_local,
                                fetch=fetch,
                                ext_ver=ExtVer)

    def log_file(self, message, terms=0, save_log_db=False):
        """
            ����������� � ����
        """

        self.result['log'] += rqu.decodeXStr(message) + krconst.kr_term_space
        while terms > 0:
            self.result['log'] += krconst.kr_term_enter
            terms -= 1
        if save_log_db:
            self.log_to_db(message, terms=terms)

        if self.log_flush:
            self.save_log_plugin(self.result['log'], c.log_info)
            self.result['log'] = ''

    def LogFile(self, message, Terms=0, SaveLogDB=False):
        """
            ����������� � ����
        """

        self.log_file(message, Terms, SaveLogDB)

    def log_to_db(self, message, terms=0):
        """
            ����������� � ��
        """

        self.result['logDB'] += rqu.decodeXStr(message) + krconst.kr_term_enter
        while terms > 0:
            self.result['logDB'] += krconst.kr_term_enter
            terms -= 1

    def save_log_plugin(self, message, typemessage='INFO'):
        # write log
        if self.base_plugin_log:
            self.base_plugin_log.write(message, krconst.kr_flag_logplugin, typemessage)

    def update_status_turn_db(self, queueid, status, params=None):
        """
            ���������� ������� �������
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
                sql_text += ' , r.starttime = DATEADD({minute} MINUTE to r.starttime) '.format(
                    minute=self.update_turn_db_time)
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
            except Exception, exc:
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
        �������� ����� � ������������
        @param filename: ��� �����
        @param flag: ����, ������ ������ ������ �� ������������ �������
        @param que_sort_id: �� ����������
        @param file_name_dest: ���� ����������� ���� ����� ���������
        @return: �� �������
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
            except Exception, exc:
                self.LogFile(exc[1])

            # ���������� ��� �������� �������
            if flag == 'CheckDir':
                sql_text = 'select R.QUEUEID from RBS_Q_CREATEQUEUE_FILENAME (?,?,?,?,?,?,?) R'
                sql_params = [res[0]['QUETASKID'], res[0]['RULE'], flag, os.path.basename(filename), 10,
                              self.parent.k_conf.global_def_dir_tmp_files, file_size]
            else:
                sql_text = 'select R.QUEUEID from Q_CREATEQUEUE_FILENAME_CONVERT (?,?,?,?,?,?,?) R'
                sql_params = [que_sort_id, flag, os.path.basename(filename), 10,
                              self.parent.k_conf.global_def_dir_tmp_files, file_size,
                              file_name_dest]

            # ������� ������� � �������� L, ��� �� ����������� ���� � ������ ����� � �������� ������ ������� � 0
            try:
                cr_queue = self.db.dbExec(sql_text,
                                          params=sql_params,
                                          fetch='many')
                return cr_queue[0]['QUEUEID']
            except Exception, exc:
                self.LogFile(exc[1])
                return None
        else:
            return None

    def is_exists_folder(self, path):
        """
            �������� �� ������������� ��������
        """

        return os.path.exists(os.path.dirname(path) + '/')

    def create_folder(self, path):
        """
            �������� ��������
        """

        if not self.is_exists_folder(path):
            os.makedirs(os.path.dirname(path))

    def copy_file(self, src_file, dst_file):
        """
            ����������� �����
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
            ��������������� �����
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
        �������� ����� + ���������� ���� �����, ���� ��� �����
        @param full_file_name: ��� �����
        @param delete_dir: ������� �������� ���������� ���� ��� �����
        @return: ���������� ��������
        """
        if self.exists_file(full_file_name, add_log=False):
            try:
                os.unlink(full_file_name)
            except:
                raise
                return False
        if delete_dir:
            ''' �������� ���� �� � �������� ��� �����'''
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
            �������� ��������
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
            ������������� �����, � ������� � ��� ��� �������������
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
            ���������� ������ � ����
        """

        self.create_folder(dst_file)
        if os.access(dst_file, os.F_OK):
            self.delete_tmp_file(dst_file)
        file_save = open(dst_file, "a")
        print >> file_save, text
        file_save.close()

    def check_dir_by_path_ext(self, path, sub_folders, mask_files='*', ignore_path=' ', mtime_from=None, mtime_to=None, ignore_file=' '):
        """
        �������� �������� �� ������� ����� �� ����� �����
        @param path: ���� � �����
        @param sub_folders: ��������� �� �����������
        @param mask_files: ����� �����
        @param ignore_path: ������������ ����
        @param mtime_from: ����������� �� ���� ��������� ����� (�) unix_timestamp
        @param mtime_to: ����������� �� ���� ��������� ����� (��) unix_timestamp
        @param ignore_file: ������������ �����
        @return:
        """

        res_file_list = []
        for mask in mask_files.split(','):
            file_list = sorted(glob.glob(path + '/' + mask), key=os.path.getmtime)
            for itm in file_list:
                if not itm.endswith('/' + ignore_path) and not itm.endswith('\\' + ignore_path):
                    if os.path.isfile(itm):
                        itm_replaced = itm.replace('\\', '/')
                        if mtime_from is not None or mtime_to is not None:
                            mtime = os.path.getmtime(itm)
                            mtime_accepted = (mtime_from is None or mtime >= mtime_from) \
                                             and (mtime_to is None or mtime <= mtime_to)
                            if mtime_accepted:
                                res_file_list.append(itm_replaced)
                        else:
                            file_name = os.path.basename(itm_replaced)
                            if not file_name.startswith(ignore_file):
                                res_file_list.append(itm_replaced)
                    elif sub_folders == '1':
                        if os.path.isdir(itm):
                            tmp_file_list = self.check_dir_by_path_ext(itm,
                                                                       sub_folders,
                                                                       mask_files=mask_files,
                                                                       ignore_path=ignore_path,
                                                                       mtime_from=mtime_from,
                                                                       mtime_to=mtime_to,
                                                                       ignore_file=ignore_file)
                            if tmp_file_list:
                                for itm_file in tmp_file_list:
                                    res_file_list.append(itm_file.replace('\\', '/'))
        return res_file_list


    def read_config_other_db(self, name_db):
        """
            ������ �������� ��� ����������� � ������ ��
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
                except Exception, exc:
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

    ''' ������ � MySQL '''
    def mysql_connect(self, name_db):
        """
            ����������� � �� MySQL
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
            ���������� �������� � MSSQL � MySQL
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

    ''' ������ ��� ������ � �������� ������������ '''
    def mount_dir(self):
        """
            ����������� �������: ����������
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
                os.system("ps -efww | grep mount | grep -v grep | awk '{print $2}' | xargs kill")
                os.system(un_mount_cmd)
            except:
                self.log_file(krconst.m_e_mount_dir % un_mount_cmd)

            try:
                os.system(mount_cmd)
            except:
                self.log_file(krconst.m_e_mount_dir % mount_cmd)

    # def create_task_email(self):
    #     """
    #     �������� ������� �� �������� ����� ��� �������������
    #     """
    #
    #     # ������� ������� ����
    #     # 1. ������� ����������� � �������
    #     # 2. ����� ���������� ������
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
    #             self.log_file('������� ������� �� �������� �����.',
    #                           terms=2,
    #                           save_log_db=True)
    def create_task_email(self):
        """
        �������� ������� �� �������� ����� ��� �������������
        """

        # ������� ������� ����
        # 1. ������� ����������� � �������
        # 2. ����� ���������� ������

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
                    self.LogFile('������� ������� �� �������� �����.',
                                 Terms=2,
                                 SaveLogDB=True)

    def create_queue_bond(self, docid, bond_type='I', auto_commit=True):
        """
        ����� ��������� � �������
        @param docid: ��������
        @param bond_type: ��� �����
        @param auto_commit: �������������� ����������
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

    def export_file(self, dir_export, file_name, doc_txt):
        """
        �������� �����
        @param file_name: ��� �����
        @param doc_txt: dict ���������
        """

        file_name = os.path.join(dir_export, file_name)
        self.log_file('��������� ����:' + file_name, terms=1)
        # if self.re_write == '0':
        #     if self.exists_file(file_name.decode('cp1251'), add_log=False):
        #         self.log_file('���� ����������, �� �������������� ���:' + file_name, terms=1)
        #         return False

        self.log_file('������� ����:' + file_name, terms=1)
        self.delete_tmp_file(file_name.decode('cp1251'))
        file_save = open(file_name.decode('cp1251'), "a")
        print >> file_save, doc_txt
        file_save.close()
        self.log_file('���������.', terms=1)

    # ������ ��� ������ � �������
    def engine_get_db(self, name_xml, layer_code=None, sql_text_add=None):
        """
        ��������� ���� ����� ������
        @param name_xml: ����������� ������ ��� ����������� � Engine
        @param layer_code: ��� ����
        @param sql_text_add: ����������� sql �����
        @return: ������ �����
        """

        result = []

        self.log_file('����������� � Engine', terms=1)
        engine_conf = kc.KConfig(name_xml)
        engine_conf.get_config_file()
        engine_conf.get_config_layer()
        engine_conf.get_config()
        db_engine = db.QueryDB(engine_conf)
        # todo ������: ����� ���������� �������� �� �������
        if db_engine.connect:
            self.log_file('���������� � Engine ������ �������', terms=1)
            # �������� ��� ���� � �������������� ���������

            sql_text = '''select l.code, u.email,  LPAD(u.sa_uid, 9, '0') as sa_uid, u.login
                                        from engine_layers l
                                             left join engine_users u on l.owner_id = u.id_user
                                        where l.code <> 'GLOBAL' '''
            sql_params = []
            if layer_code:
                sql_text += ' and l.code=? '
                sql_params = [layer_code]
            if sql_text_add:
                sql_text += sql_text_add

            res = self.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   db_local=db_engine,
                                   fetch='many'
                                   )
            if res['status'] == c.kr_sql_error:
                self.log_file('������ ��������� ���� �����', terms=1)
            else:
                result = res['datalist']
        else:
            self.log_file('������ ����������� � Engine', terms=1)
        return result, engine_conf
