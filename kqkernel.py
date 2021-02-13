# -*- coding: utf-8 -*-
"""
    swat 11.12.2014
    version 0.0.2.3
    Ядро сервера-задач
"""

import time
import threading
import os
import sys
import os.path

import krconst
import krconst as k
import krconst as c
import queue_conf as db_conf
import kconfig as conf
import utils.file as f
import datetime as dt
import time as t
import queue_db as db
import rbsqutils as rqu
from rbsqutils import current_time
from rbsqutils import current_time_zone

from BasePRLogger import RPLogger
from kqevents import FBEventThread
from utils.decorator import synchronized

VERSION = '0.0.3.0'


class Layer(object):
    """
        Класс проверки на параметры работы сервера-задач
        1. Обычнный режим
        2. работа со слоями
    """

    __db_engine = None
    engine_conf = None
    db_user = None
    db_pass = None

    def __init__(self, name_engine_db, group_name, db_user='SYSDBA', db_pass='masterkey'):
        self.group_name = None
        self.db_user = db_user
        self.db_pass = db_pass
        self.engine_conf = conf.KConfig(name_engine_db)
        ''' проверим в каком режиме должен работать сервер-задач  '''
        self.engine_conf.get_config_file()
        self.engine_conf.get_config_layer()
        if self.engine_conf.layers_work:
            self.engine_conf.get_config()
            if db_user:
                self.engine_conf.db_user = db_user
            if db_pass:
                self.engine_conf.db_pass = db_pass
            ''' Подключимся к Engine для получения параметров работы '''
            self.__db_engine = db.QueryDB(self.engine_conf)
            if self.__db_engine.connect:
                self.group_name = group_name
                layers_db = self.__layers_get_info(group_name)
                if layers_db:
                    self.__layers_start(layers_db, self.engine_conf, db_user, db_pass)
                    self.__subscribe_event()
                else:
                    print(c.m_e_layer_get_layers % group_name)
            else:
                print(c.m_e_layer_not_db_engine % self.engine_conf.db_path)
        else:
            print(c.m_e_layer_config)

    def __layers_get_info(self, group_name):
        """
            Получение слоев
        """
        print(group_name)

        sql_text = 'select * from QUE_LAYERS_GET_BY_GROUP(?) l'
        sql_params = [group_name]
        res = self.__db_engine.dbExec(sql_text,
                                      params=sql_params,
                                      fetch='many')
        return res

    def __layers_start(self, layers_db, engine_conf, db_user, db_pass):
        """
            Запуск слоя
        """

        for layer in layers_db:
            self.__layer_start(layer['LAYER_CODE'],
                               layer['SERVER_CODE'],
                               engine_conf,
                               db_user,
                               db_pass,
                               layer['LB_SN_DOMAIN'])

    @staticmethod
    def __layer_start(layer_code, server_code, engine_conf, db_user, db_pass, sn_name):
        """
        Старт слоя
        @param layer_code:
        @param server_code:
        @param engine_conf:
        @param db_user:
        @param db_pass:
        @param sn_name: имя серверада Нод
        @return:
        """
        thread_kernel = QKernelThread(layer_code,
                                      server_code,
                                      engine_conf,
                                      db_user,
                                      db_pass,
                                      sn_name)
        thread_kernel.setName('MainThread' + layer_code)
        thread_kernel.start()

    def __subscribe_event(self):
        """
        Подписка на событие
        @return:
        """
        thread = FBEventThread(self.__db_engine.connect, 'ENGINE_QUE_' + self.group_name.upper(), self.__fb_event_callback)
        thread.setName('EventThread' + self.group_name)
        # thread.setDaemon(True)
        thread.start()

    def __fb_event_callback(self):
        """
        Поменялись слои для робота, пробуем
        @return:
        """
        # Список актуальных слоев
        layers_from_db = self.__layers_get_info(self.group_name)
        # Список слоев, которые уже работают
        exists_layers = []
        for itm in threading.enumerate():
            task_name = itm.getName()
            if task_name != 'MainThread' and task_name.startswith('MainThread'):
                layer_code = task_name.replace('MainThread', '')
                exists_layers.append(layer_code)
        # Проверяем новые и запускаем
        if layers_from_db is not None:
            for layer in layers_from_db:
                layer_code = layer['LAYER_CODE']
                try:
                    index = exists_layers.index(layer_code)
                except ValueError:
                    index = -1
                if index < 0:
                    # Новый слой
                    print('Layer %s is going to start by event' % layer_code)
                    self.__layer_start(layer_code,
                                       layer['SERVER_CODE'],
                                       self.engine_conf,
                                       self.db_user,
                                       self.db_pass,
                                       layer['LB_SN_DOMAIN'])
                else:
                    exists_layers.remove(layer_code)
        # В exists_layers остались слои для остановки
        for itm in threading.enumerate():
            task_name = itm.getName()
            if task_name != 'MainThread' and task_name.startswith('MainThread'):
                layer_code = task_name.replace('MainThread', '')
                try:
                    index = exists_layers.index(layer_code)
                except ValueError:
                    index = -1
                if index > -1:
                    print('Layer %s is going to stop by event' % layer_code)
                    itm.kill()
                    exists_layers.remove(layer_code)


class QKernelThread(threading.Thread):
    """
        Класс запуска потока сервера-задач
    """

    __server_code = None
    __db_user = None
    __db_pass = None
    __name_db = None
    __layer_conf = None
    __kill_event = None

    def __init__(self, name_db, server_code, layer_conf, db_user, db_pass, sn_name):

        self.__name_db = name_db
        self.__server_code = server_code
        self.__layer_conf = layer_conf
        self.__db_user = db_user
        self.__db_pass = db_pass
        self.__sn_name = sn_name
        self.__kill_event = threading.Event()

        threading.Thread.__init__(self)

    def run(self):
        print('db/layer=%s, server code=%s' % (self.__name_db, self.__server_code) + c.kr_term_enter)
        try:
            a = QKernel(self.__name_db,
                        self.__server_code,
                        db_user=self.__db_user,
                        db_pass=self.__db_pass,
                        layer_code=self.__name_db,
                        layer_conf=self.__layer_conf,
                        run_server=False,
                        sn_name=self.__sn_name)
            a.run(kill_event=self.__kill_event)
        except:
            print(rqu.TracebackLog(''))

    def kill(self):
        self.__kill_event.set()

q_kernel_db_lock = threading.Lock()


class QKernel(object):

    k_conf = None
    Logger = None
    q_server_id = None
    q_name = None
    layer_code = None
    layer_conf = None

    time_zone = 0

    def __init__(self, name_db, server_code, db_user='DBADMIN', db_pass='0', layer_code='', layer_conf=None,
                 run_server=True, sn_name=None):
        self.db = None
        self.name_db = name_db
        self.server_code = server_code
        self.db_user = db_user
        self.db_pass = db_pass

        self.layer_code = layer_code
        self.layer_conf = layer_conf
        self.sn_name = sn_name

        if self.sn_name:
            if not self.sn_name.startswith('http'):
                self.sn_name = 'https://' + self.sn_name
            if not self.sn_name.endswith('/'):
                self.sn_name += '/'

        ''' array of task '''
        self.taskQueue = []
        ''' array lock quetask '''
        self.lockQueue = {}
        if self.config_read():
            if not self.k_conf.os_version:
                print('Error get OS version')
                return None
            if run_server:
                self.run()

    def run(self, kill_event=None):
        """
        Внешний запуск сервера, не из конструктора
        @return:
        """
        self.start_server()
        if self.k_conf.status_server_queue == krconst.kr_statusserver_lostconnect:
            print('Error connect to DB')
        else:
            while self.k_conf.status_server_queue != krconst.kr_statusserver_close:
                if kill_event is not None:
                    # Сработала остановка потока
                    if kill_event.isSet():
                        self.stop_server()
                        break
                time.sleep(self.k_conf.global_sleep_interval)

    def config_read(self):
        self.k_conf = conf.KConfig(self.name_db)
        self.k_conf.get_os_version()
        if self.k_conf:
            self.k_conf.get_config_file()
            self.k_conf.get_config(self.layer_code, self.layer_conf)
            if self.db_user:
                self.k_conf.db_user = self.db_user
            if self.db_pass:
                self.k_conf.db_pass = self.db_pass
            return True
        else:
            return False

    def connect_db_server(self):
        """
            Подключение к БД
        """

        if not self.db:
            self.db = db.QueryDB(self.k_conf)

    def get_server_id(self):
        """
            Получение id сервера
        """

        sql_text = 'select rs.queserverid, rs.name, rs.breakenabled, rs.breakfrom, ' \
                   'rs.breakto, rs.logfilename, rs.logrenewnum ' \
                   'from RBS_Q_GETSERVERID(?) rs'
        sql_params = [self.server_code]
        res = self.db.dbExec(sql_text,
                             params=sql_params,
                             fetch='one')
        if res:
            self.q_server_id = res['queserverid']
            self.q_name = res['name']

            if res['logfilename'] and self.k_conf.global_log == 1:
                log_file_name = res['logfilename'].replace('\\', '/')
                if self.layer_code != '':
                    log_file_name = log_file_name.replace('./log/', './log/' + self.layer_code + '/')
                self.Logger = RPLogger(cfg=self.k_conf,
                                       path=log_file_name,
                                       file_size=res['logrenewnum'])

            self.k_conf.break_params['breakenabled'] = res['breakenabled']
            if res['breakenabled'] == '1':
                self.k_conf.break_params['breakfrom'] = res['breakfrom']
                self.k_conf.break_params['breakto'] = res['breakto']
                self.k_conf.break_params['timebreakenabled'] = res['breakto'] - res['breakfrom']
        else:
            self.q_server_id = None
        return self.q_server_id

    def restart_dangling_turn(self):
        """
            Восстанавливаем задания после перезапуска
        """

        try:
            sql_text = 'execute procedure RBS_Q_RESTARTDANGLING(?)'
            sql_params = [self.q_server_id]
            self.db.dbExec(sql_text,
                           params=sql_params,
                           fetch='None')
        except Exception as exc:
            self.log_server('Ошибка при восстановлении заданий ' + exc[1], krconst.log_error)

    def start_server(self):
        """
            Старт сервера
        """
        self.connect_db_server()
        if self.db:
            if self.db.connect:
                self.k_conf.LostConnectDB = False
                self.k_conf.status_server_queue = krconst.kr_statusserver_work
                if self.get_server_id():
                    if self.layer_code is not None:
                        if self.k_conf.global_dir_tmp_clear:
                            layer_tmp_path = os.path.join(self.k_conf.global_def_dir_tmp_files, self.layer_code)
                            mtime = dt.datetime.now() - dt.timedelta(days=self.k_conf.global_def_dir_tmp_clear_interval)
                            mtime_to = t.mktime(mtime.timetuple())
                            tmp_files = f.check_dir_by_path(layer_tmp_path, '1', mtime_to=mtime_to)
                            for tmp_file in tmp_files:
                                f.delete_tmp_file(tmp_file, delete_dir=True)
                    else:
                        self.log_server(krconst.kr_message_startserver % self.q_name, krconst.log_info)
                        ''' Check and unlink old sticker files '''
                        res = rqu.delete_files_by_mask(self.k_conf.global_def_dir_tmp_files,
                                                       '*.pdf',
                                                       current_time(self.db, self.layer_code))
                        if not res:
                            self.log_server('Old stickers files have cleaned successful', krconst.log_info)
                        else:
                            self.log_server(res, krconst.log_info)
                    ''' Restart dangling turn '''
                    self.restart_dangling_turn()
                    ''' update status server '''
                    self.register_status_server('1')
                    self.taskQueue = self.get_queue_task()
                    if self.taskQueue:
                        self.create_queue_task()
                else:
                    print(k.m_e_server_code_is_none)
            else:
                self.k_conf.status_server_queue = krconst.kr_statusserver_lostconnect
                self.k_conf.LostConnectDB = True
                ''' если первый запуск то лога еще нет, и тогда печатаем в консоль, что бы понять суть ошибки '''
                try:
                    if self.Logger:
                        self.log_server(self.db.db_message, krconst.log_error)
                    else:
                        print(self.db.db_message)
                except:
                    print(self.db.db_message)
        else:
            print(k.m_e_db_none)

    def stop_server(self, is_break=False):
        """
            Остановка сервера
        """

        if self.db:
            ''' update status server '''
            if is_break:
                self.register_status_server('B')
            else:
                self.register_status_server('0')
            self.db.close_connect()
        self.db = None

        ''' stop Thread Plugin and Guard '''
        stop_threads = []
        for itm in threading.enumerate():
            task_name = itm.getName()
            if not task_name.startswith('pydev'):
                if not task_name.startswith('MainThread'):
                    if not task_name.startswith('GuardConnect') and not task_name.startswith('EventThread') and \
                            (self.layer_code == '' or task_name.endswith(self.layer_code)):
                        stop_threads.append({'name': task_name, 'item': itm})
                        # itm.kill()
                        # self.log_server('Stop QueueTask: ' + task_name, krconst.log_info)
                    if (self.k_conf.status_server_queue not in (krconst.kr_statusserver_lostconnect,
                                                                krconst.kr_statusserver_break)) \
                            and (task_name.startswith('GuardConnect'))\
                            and (self.layer_code == '' or task_name.endswith(self.layer_code)):
                        # Первым должен останавливаться guard
                        stop_threads.insert(0, {'name': task_name, 'guard': True, 'item': itm})
                        # itm.kill()
                        # self.log_server('Stop GuardConnect: ' + task_name, krconst.log_info)
        for thread_info in stop_threads:
            is_guard = 'guard' in thread_info and thread_info['guard'] == True
            thread_info['item'].kill()
            if is_guard:
                msg = 'Stop GuardConnect: ' + thread_info['name']
            else:
                msg = 'Stop QueueTask: ' + thread_info['name']
            self.log_server(msg, krconst.log_info)

    def get_queue_task(self):
        """
            получение задач, и создание списка
        """

        sql_text = 'select rt.tasktype, rt.code, rt.name, rt.autorun , rt.modulename, rt.quetaskid, rt.moduleid,' \
                   ' rt.pollinterval, rt.params, rt.actionsparams, rt.filename, rt.logfilename, rt.delsuccessfiles,' \
                   ' rt.logrenewnum, rt.quantity, rt.logextended from RBS_Q_GETQUEUETASK(?) rt'
        sql_params = [self.q_server_id]
        res = self.db.dbExec(sql_text,
                             params=sql_params,
                             fetch='many')
        if res:
            lst = []
            for item in res:
                log_file_name = item['logfilename']
                if self.layer_code != '':
                    log_file_name = log_file_name.replace('./log/', './log/' + self.layer_code + '/')
                lst.append({
                    'quetaskid': int(item['quetaskid']),
                    'autorun': item['autorun'],
                    'startQueue': None,
                    'endQueue': None,
                    'starttimne': None,
                    'endtime': None,
                    'moduleid': int(item['quetaskid']),
                    'tasktype': item['tasktype'],
                    'taskcode': item['code'],
                    'interval': int(item['pollinterval']),
                    'taskparams': item['params'],
                    'taskactionsparams': item['actionsparams'],
                    'filemodule': item['filename'],
                    'logfilename': log_file_name,
                    'name': item['name'],
                    'delsuccessfiles': item['delsuccessfiles'],
                    'logrenewnum': item['logrenewnum'],
                    'logextended': item['logextended'],
                    'quantitytread': int(item['quantity']),
                    'layer_code': self.layer_code,
                    'sn_name': self.sn_name
                })
            return lst

    def create_queue_task(self):
        """
            Запуск задач
        """
        self.time_zone = current_time_zone(self.db, self.layer_code)
        cur_time = current_time(self.db, self.layer_code)
        if (self.k_conf.break_params['breakenabled'] == '0') or \
                (self.k_conf.break_params['breakenabled'] == '1' and
                    (self.k_conf.break_params['breakfrom'] > cur_time or
                     self.k_conf.break_params['breakto'] < cur_time)):
            self.lockQueue = {}
            for itm in self.taskQueue:
                if itm['autorun'] == '1':
                    if itm['quantitytread'] > 1:
                        self.lockQueue[itm['taskcode']] = threading.RLock()
                        cnt_tread = 0
                        while cnt_tread < itm['quantitytread']:
                            q_thread = QueueThread(self, itm)
                            q_thread.setName(itm['taskcode'] + self.layer_code + str(cnt_tread))
                            q_thread.setDaemon(True)
                            q_thread.start()
                            self.log_server(krconst.kr_message_startQueueTask % q_thread.getName(), krconst.log_info)
                            cnt_tread += 1
                    else:
                        q_thread = QueueThread(self, itm)
                        q_thread.setName(itm['taskcode'] + self.layer_code)
                        q_thread.setDaemon(True)
                        q_thread.start()
                        self.log_server(krconst.kr_message_startQueueTask % q_thread.getName(), krconst.log_info)

        ''' Зауск guard '''
        guard_connect = False
        for itm in threading.enumerate():
            if itm.getName() == 'GuardConnect' + self.layer_code:
                guard_connect = True

        if not guard_connect:
            params_g = []
            params_g.append({
                'layer_code': self.layer_code,
                'db': self.db
            })
            g_thread = GuardDBThread(self, params_g[0])
            g_thread.setName('GuardConnect' + self.layer_code)
            g_thread.setDaemon(True)
            g_thread.start()
            self.log_server(krconst.kr_message_startQueueTask % g_thread.getName(), krconst.log_info)

    def log_server(self, message, type_message='INFO'):
        """
            Запись лога работы сервера
        """

        if self.k_conf.global_log == 1:
            self.Logger.write(message, krconst.kr_flag_logglobal, type_message)

    @synchronized(q_kernel_db_lock)
    def register_status_server(self, status):
        """
            Обновление статуса сервера
        """

        if self.k_conf.status_server_queue != krconst.kr_statusserver_lostconnect and self.db is not None:
            try:
                sql_text = 'execute procedure RBS_Q_SERVERSTATUS(?,?)'
                sql_params = [self.q_server_id, status]
                self.db.dbExec(sql_text,
                               params=sql_params,
                               fetch='none')
            except Exception as exc:
                self.log_server(krconst.kr_message_error_updateserverstatus, krconst.log_error)
                if self.db is not None and self.db.db_message:
                    self.log_server(self.db.db_message)
                else:
                    self.log_server(exc[1])


class Kill(Exception):
    pass


class QueueThread(threading.Thread):
    def __init__(self, parent, params):
        self.parent = parent
        self.params = params
        self.dbplug = None
        self.runPlugin = True
        self.stopPlugin = False
        self.ConnectLost = False
        self.TaskRule = False

        # список для проверки версий файлов сервера-задач
        self.versionfile = []

        threading.Thread.__init__(self)

    def run(self):
        if not self.dbplug:
            self.dbplug = db.QueryDB(self.parent.k_conf)
            if not self.dbplug.connect:
                ''' Ошибка открытия подключения к БД '''
                self.parent.log_server(krconst.kr_message_error_openconnect % self.params['name'], c.log_error)
            else:
                self.check_turn()

    def kill(self):
        # Kill()
        self.stopPlugin = True

    def check_turn(self):
        """
            Проверка на наличие задания для задачи
        """

        while self.runPlugin \
                and self.parent.k_conf.status_server_queue in (krconst.kr_statusserver_start, c.kr_statusserver_work):
            res = None
            if self.params['tasktype'] != '0':
                try:
                    sql_text = 'select r.queueid, r.rule from RBS_Q_LISTNEXTQUEUE (?, ?) R'
                    sql_params = [self.params['quetaskid'], 'C']
                    res = self.dbplug.dbExec(sql_text,
                                             params=sql_params,
                                             fetch='many')
                except:
                    self.TracebackLog(krconst.m_e_exec_proc_name % 'RBS_Q_LISTNEXTQUEUE')
                    res = None
                if self.params['tasktype'] == 'E' and res:
                    self.TaskRule = res[0]['rule']
            if self.dbplug.db_message.find(krconst.m_e_unable_to_complete) != -1:
                self.ConnectLost = True
            if res or self.params['tasktype'] == '0':
                self.runPlugin = self.run_plugin()

            ''' Потеря подключения к БД '''
            if self.ConnectLost:
                self.runPlugin = False
                self.parent.k_conf.LostConnectDB = True
            # Перед перерывом проверяем, чтобы не держал ресурсы
            if self.stopPlugin:
                self.runPlugin = False
            if self.runPlugin:
                self.sleep_turn()
                # После перерыва, если что-то изменилось за время перерыва
                if self.stopPlugin:
                    self.runPlugin = False
        if self.dbplug:
            self.dbplug.close_connect()

    def sleep_turn(self):
        time.sleep(self.params['interval'])

    def run_plugin(self):
        """
            Запуск плагина
        """

        ''' Если указан модуль, то просто импортируем его. Данную задачу обрабатывает один модуль '''
        if self.params['filemodule']:
            filename = (self.params['filemodule']).split('.')[0]
            try:
                mod = self.importer(self.parent.k_conf.main_path + '/plugins/' + filename)
            except:
                self.TracebackLog('Error execute plugin ' + filename + ', taskcode=' + self.params['taskcode'])
                return False
        ''' модуля нет, и задание не типа импорт/экспорт то ошибка. '''
        if not self.params['filemodule'] and \
                self.params['tasktype'] not in ('I', 'E'):
            self.parent.log_server(krconst.m_e_setting_task % self.params['taskcode'], krconst.log_error)
            return False

        run_turn = True
        while run_turn:
            res = None
            queueid = None
            if self.params['tasktype'] != '0':
                res = self.get_next_queue(self.dbplug, self.params)
                if res:
                    queueid = res['queueid']

            if self.dbplug.db_message.find(krconst.m_e_unable_to_complete) != -1:
                self.ConnectLost = True
            if (res) or (self.params['tasktype'] == '0'):
                try:
                    ''' Непосредственный запуск плагина '''
                    if self.params['tasktype'] == '0':
                        resqueue = None
                    else:
                        resqueue = res

                    self.PlugParams = ({'parent': self.parent,
                                        'taskparams': self.params,
                                        'db': self.dbplug,
                                        'queueid': queueid,
                                        'resqueue': resqueue})
                    ''' проверяем если нет модуля и тип импорт/экспорт, то по правилу пробуем определить модуль '''
                    if not self.params['filemodule'] \
                            and self.params['tasktype'] in ('I', 'E'):
                        if resqueue['rulemodulefilename']:
                            filename = (resqueue['rulemodulefilename']).split('.')[0]
                            try:
                                mod = self.importer(self.parent.k_conf.main_path + '/plugins/' + filename)
                            except:
                                self.TracebackLog('Error execute plugin ' + filename + ', taskcode=' + self.params['taskcode'])
                                return False
                        else:
                            ''' по правилу не нашли нужный модуль '''
                            self.parent.log_server(krconst.m_e_setting_task % self.params['taskcode'], krconst.log_error)
                            return False
                    plugin = mod.Plugin(self.PlugParams)

                    if plugin.result['result'] != krconst.plugin_error:
                        plugin.run()
                        plugin.EndRunPlugin()
                        # if task finish OK, then status = 1 else E
                        # to do message to DB
                        if self.params['tasktype'] != '0':
                            #if plugin.result['result'] == krconst.plugin_ok:
                            #    self.UpdateStatusTurnDB(res['queueid'], '1')
                            if plugin.result['result'] == krconst.plugin_error:
                                self.update_status_turn_db(res['queueid'], 'E')
                            #if plugin.result['result'] == krconst.plugin_restart:
                            #    self.UpdateStatusTurnDB(res['queueid'], '0')
                    else:
                        plugin.EndRunPlugin()
                        if queueid:
                            self.update_status_turn_db(res['queueid'], 'E')
                    del plugin
                except Exception as exc:
                    self.TracebackLog('Error execute plugin ' + filename + ', taskcode=' + self.params['taskcode'])
                    self.TracebackLog(exc)
                    try:
                        if res['queueid']:
                            self.update_status_turn_db(res['queueid'], 'E')
                    except:
                        pass
            if not res or self.params['tasktype'] == '0' or self.ConnectLost:
                run_turn = False
        return True

    def importer(self, location):
        """
        Импорт модулей
        @param location: полный путь
        @return:
        """

        if not os.access(location + '.py', os.F_OK) and not os.access(location + '.pyc', os.F_OK):
            self.parent.log_server('Не найден модуль: ' + location + '.py', krconst.log_error)
            return None
        (head, tail) = os.path.split(location)
        sys.path[0:0] = [head]
        try:
            result = __import__(tail)
        except Exception as exc:
            # todo Щеглов: тут почему то нет вывода в лог, не понятно вообще
            self.parent.log_server(exc)
            print(exc)
            return None

        # проверка версионности
        #if self.VesionPlugin(location):
        #    try:
        #        version = result.version
        #    except:
        #        version = krconst.kr_message_warning_version_notsupport
        #    self.parent.LogServer(location + '.py version=' + version, krconst.log_info)

        del sys.path[0]
        return result

    def VesionPlugin(self, module):
        """
            Заполнение списка с плагинами для определения версионности
        """

        for itm in self.versionfile:
            if itm['module'] == module:
                return False
        self.versionfile.append({'module': module})
        return True

    def get_next_queue(self, db, params):
        """
            проверяем если задача многопоточна то лочим объект словаря,
            чтобы не получить один и тот же ид задания, для разных потоков
        """

        if params['quantitytread'] > 1:
            self.parent.lockQueue[params['taskcode']].acquire()
        try:
            sql_text = 'select * from RBS_Q_LISTNEXTQUEUE (?, ?) R'
            sql_params = [params['quetaskid'], 'G']
            res_queue = db.dbExec(sql_text,
                                  params=sql_params,
                                  fetch='one')
            if res_queue:
                if not self.update_status_turn_db(res_queue['queueid'], 'R'):
                    return None
        except:
            self.TracebackLog(krconst.m_e_exec_proc_name % 'RBS_Q_LISTNEXTQUEUE')
            res_queue = None
        if params['quantitytread'] > 1:
            self.parent.lockQueue[params['taskcode']].release()
        return res_queue

    def update_status_turn_db(self, queueid, status):
        """
            update task status"
            R - task is run now
            1 - task finish correctly
            E - task finish with error
        """

        if status in ['E', 'R']:
            try:
                res = self.dbplug.dbExec('''update r_queue r set r.status = ? where r.queueid = ?''',
                                         params=[status, queueid], fetch='None')
                return True
            except:
                self.TracebackLog('[status,queueid] = [' + status + ', ' + str(queueid) + ']')
                return False
        else:
            self.parent.log_server('Invalid status for update r_queue', krconst.log_error)
            return False

    def TracebackLog(self, message):
        message = rqu.TracebackLog(message)
        self.parent.log_server(message, krconst.log_error)


class GuardDBThread(threading.Thread):
    def __init__(self, parent, params):
        self.parent = parent
        self.guard = True
        self.dbplug = None
        self.params = params
        threading.Thread.__init__(self)

    def run(self):
        while self.guard:
            # check for break
            cur_time = current_time(self.params['db'], self.params['layer_code'], self.parent.time_zone)
            if self.parent.k_conf.break_params['breakenabled'] == '1':
                if self.parent.k_conf.break_params['breakfrom'] <= cur_time \
                        and self.parent.k_conf.break_params['breakto'] >= cur_time \
                        and self.parent.k_conf.status_server_queue == krconst.kr_statusserver_work:
                    self.parent.k_conf.status_server_queue = krconst.kr_statusserver_break
                    self.parent.log_server(krconst.kr_message_startbreake, krconst.log_info)
                    self.parent.stop_server(is_break=True)
                    self.dbplug = db.QueryDB(self.parent.k_conf)
                    if self.dbplug.connect:
                        self.dbplug.close_connect()
                else:
                    if (self.parent.k_conf.break_params['breakfrom'] > cur_time \
                            or self.parent.k_conf.break_params['breakto'] < cur_time) \
                            and self.parent.k_conf.status_server_queue == krconst.kr_statusserver_break:
                        self.parent.log_server(krconst.kr_message_stopbreake, krconst.log_info)
                        self.TryConnect()

            if self.parent.k_conf.status_server_queue in (krconst.kr_statusserver_work, krconst.kr_statusserver_lostconnect):
                if self.parent.k_conf.LostConnectDB:
                    self.parent.k_conf.status_server_queue = krconst.kr_statusserver_lostconnect
                    self.parent.stop_server()
                    # try to connect
                    self.TryConnect()
            #else:
            #    self.guard = False
            time.sleep(db_conf.kr_guard_timer)
            if self.guard and self.parent.k_conf.status_server_queue == krconst.kr_statusserver_work:
                # update status server
                self.parent.register_status_server('1')

    def TryConnect(self):
        if not self.guard:
            return
        self.dbplug = db.QueryDB(self.parent.k_conf)
        if self.dbplug.connect:
            self.dbplug.close_connect()
            self.dbplug = None
            self.parent.k_conf.status_server_queue = krconst.kr_statusserver_work
            self.parent.start_server()
        else:
            if self.dbplug.db_message != '':
                self.parent.log_server(self.dbplug.db_message, krconst.log_error)

    def kill(self):
        self.guard = False
        # Kill()
