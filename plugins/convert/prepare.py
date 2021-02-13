# -*- coding: utf-8 -*-

"""
    Модуль предварительной подготовки файлов для конвертации в формат json
    из других форматов
    На данный момент обрабатываются маски файлов json и xml
"""

import os
import json
import requests
import shutil
import re
import xml.etree.ElementTree as Et

import krconst as c
import BasePlugin as Bp

from utils.file import check_dir_by_path


class Plugin(Bp.BasePlugin):
    """
    Класс предварительной обработки файлов
    """
    iter_num = 0  # Статическая переменная, количество запусков задачи
    sa_url = 'http://in.litebox.ru'
    sa_username = '_system_admin_pro'
    sa_password = '98b705ac3'
    sa_token = None

    def run(self):
        """
            Запуск плагина на исполнение
        """

        if self.iter_num % 15 == 0:  # Каждые 15 запусков проверяем переезд слоя и сбрасываем счетчик
            self.check_relocate()
            self.iter_clear()
        self.iter()

        if self.taskparamsxml:
            sub_folders = '1'
            mask_files = '*'

            # получим список типов обмена
            sql_text = 'select distinct qc.type_exchange from q_exchange_convert qc'
            type_exchange = self.execute_sql(sql_text,
                                             sql_params=[],
                                             fetch='many')
            if type_exchange['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения типов обмена', terms=1)
            else:
                for type_exc in type_exchange['datalist']:
                    # путь где лежит папка для внешнего обмена
                    type_dir_storage = self.parser_xml(self.taskparamsxml,
                                                       type_exc['type_exchange'] + '_dir_storage').replace('\\', '/')
                    # путь где лежит папка для внутреннего обмена
                    type_dir_iner = self.parser_xml(self.taskparamsxml,
                                                    type_exc['type_exchange'] + '_dir_iner').replace('\\', '/')

                    # получим имена папок для сканирования
                    sql_text = 'select * from q_exchange_convert qc where qc.type_exchange =? order by qc.code_user'
                    sql_params = [type_exc['type_exchange']]
                    res_code = self.execute_sql(sql_text,
                                                sql_params=sql_params,
                                                fetch='many')
                    if res_code['status'] == c.kr_sql_error:
                        self.log_file('Ошибка получения списка входящих каталогов для сканирования.', terms=1)
                    else:
                        for res_code_user in res_code['datalist']:
                            # Валидация
                            if 'fileextensions' in res_code_user:
                                extensions = res_code_user['fileextensions']
                            else:
                                extensions = None

                            sql_rules_text = 'select qesf.extension, qesf.id as file_id, qesf.mask, qesk.id as key_id, qesk.key ' \
                                        'from q_exchange_convert_file qcf ' \
                                        'left join q_exchange_file_key qefk on qefk.id = qcf.file_key_id ' \
                                        'left join q_exchange_sp_key qesk on qesk.id = qefk.key_id ' \
                                        'left join q_exchange_sp_file qesf on qesf.id = qefk.file_id ' \
                                        'where qcf.exchange_convert_id = ?'

                            sql_rules_params = [res_code_user['id']]
                            res_rulses_code = self.execute_sql(sql_rules_text,
                                                        sql_params=sql_rules_params,
                                                        fetch='many')

                            dataRules = {'keys': {}, 'masks': {}, 'extensions': {}}
                            # проверка правил
                            for res_rule in res_rulses_code['datalist']:
                                dataRules['keys'][res_rule['key_id']] = res_rule['key']
                                rule_mask = res_rule['mask']
                                if (rule_mask != ""):
                                    rule_mask = rule_mask.replace("*", ".*")
                                    rule_mask = rule_mask.replace("?", "_")
                                dataRules['masks'][res_rule['file_id']] = {'pattern': rule_mask, 'name':res_rule['mask']}
                                dataRules['extensions'][res_rule['file_id']] = res_rule['extension']

                            # Работа с входящими файлами
                            path_search = os.path.join(type_dir_storage, res_code_user['code_user'])
                            path_search = os.path.join(path_search, 'out')
                            path_search = path_search.replace('\\', '/')
                            self.log_file('Проверка каталога: ' + path_search, terms=1)
                            # проверим существует ли данный каталог если нет, то предупреждение в лог файл
                            if not self.is_exists_folder(path_search):
                                self.log_file(c.m_e_not_exists_folder % path_search + c.t_enter)
                                self.result['result'] = c.plugin_error
                            else:
                                # проверяем входящие от клиента
                                # у клиента out но для нас это in поэтому необходимо менять
                                file_list = check_dir_by_path(path_search,
                                                              sub_folders,
                                                              mask_files=mask_files,
                                                              ignore_file='temp')
                                for itm in file_list:
                                    valid, valid_error = self.validate(itm, extensions, dataRules)
                                    if not valid:
                                        self.to_error(itm, valid_error)
                                    else:
                                        # получим каталог в котором находится файл
                                        dir_name = os.path.dirname(itm)
                                        file_name = os.path.basename(itm)
                                        path_add = dir_name.replace(path_search, '')
                                        if path_add.startswith('/'):
                                            path_add = path_add[1:]

                                        dst_file = os.path.join(res_code_user['cn_dir'], res_code_user['layer_code'])
                                        dst_file = os.path.join(dst_file, 'in')
                                        dst_file = os.path.join(dst_file, path_add)
                                        dst_file = os.path.join(dst_file, file_name)

                                        if res_code_user['type_convert'] == 'JSON':
                                            # родной формат, просто скопируем файл нужны каталог
                                            self.move_file(itm, dst_file)
                                        elif res_code_user['type_convert'] == 'CRJSON':
                                            # создаем задание на конвертацию файла из формата RARUS
                                            file_queueid = self.check_file_in_queue_sort(itm,
                                                                                         'Convert',
                                                                                         res_code_user['quesortid'],
                                                                                         os.path.dirname(dst_file))
                                            if file_queueid:
                                                copy_dist = os.path.join(self.parent.k_conf.global_def_dir_tmp_files,
                                                                         'Q' + str(file_queueid))
                                                copy_dist = os.path.join(copy_dist, file_name)
                                                if self.copy_file(itm, copy_dist):
                                                    self.log_file(c.m_i_file_create_task_ok % itm.replace('\\', '/'))
                                                    self.update_status_turn_db(file_queueid, c.kr_status_new)
                                                    if not self.delete_tmp_file(itm):
                                                        self.log_file(c.m_e_delete_file % itm.replace('\\', '/'))
                                                        self.result['result'] = c.plugin_error
                                                        return False

                            # Работа с входящими VikiMini
                            path_search = os.path.join(type_dir_storage, res_code_user['code_user'])
                            path_search = os.path.join(path_search, 'vikimini')
                            path_search = path_search.replace('\\', '/')
                            self.log_file('Проверка каталога: ' + path_search, terms=1)
                            # проверим существует ли данный каталог если нет, то предупреждение в лог файл
                            if not self.is_exists_folder(path_search):
                                self.log_file(c.m_e_not_exists_folder % path_search + c.t_enter)
                                self.result['result'] = c.plugin_error
                            else:
                                file_list = check_dir_by_path(path_search,
                                                              sub_folders,
                                                              mask_files=mask_files,
                                                              ignore_file='temp')
                                self.log_file('Файлы для облака от vikimini:', terms=1)
                                self.log_file(file_list, terms=1)
                                for itm in file_list:
                                    # получим каталог в котором находиться файл
                                    dir_name = os.path.dirname(itm)
                                    file_name = os.path.basename(itm)
                                    path_add = dir_name.replace(path_search, '')
                                    if path_add.startswith('/'):
                                        path_add = path_add[1:]

                                    # нам нужны только файлы с отчетами
                                    self.log_file('Файл для обработки:', terms=1)
                                    self.log_file(file_name, terms=1)
                                    if file_name == 'report.txt':

                                        dst_file = os.path.join(res_code_user['cn_dir'], res_code_user['layer_code'])
                                        dst_file = os.path.join(dst_file, 'in')
                                        dst_file = os.path.join(dst_file, path_add)
                                        dst_file = os.path.join(dst_file, file_name)
                                        if not self.exists_file(dst_file, add_log=False):
                                            self.move_file(itm, dst_file)
                                    else:
                                        self.log_file('Не подходит!', terms=1)

                            # Работа с исходящими файлами
                            path_search = os.path.join(type_dir_iner, res_code_user['cn'])
                            path_search = os.path.join(path_search, res_code_user['layer_code'])
                            path_search = os.path.join(path_search, 'out')
                            path_search = path_search.replace('\\', '/')
                            self.log_file('Проверка каталога: ' + path_search, terms=1)
                            # проверим существует ли данный каталог если нет, то предупреждение в лог файл
                            if not self.is_exists_folder(path_search):
                                self.log_file(c.m_e_not_exists_folder % path_search + c.t_enter)
                                self.result['result'] = c.plugin_error
                            else:
                                file_list = check_dir_by_path(path_search,
                                                              sub_folders,
                                                              mask_files=mask_files,
                                                              ignore_file='temp')
                                for itm in file_list:
                                    # получим каталог в котором находиться файл
                                    dir_name = os.path.dirname(itm)
                                    file_name = os.path.basename(itm)

                                    cn = res_code_user['cn']
                                    layer_code = res_code_user['layer_code']
                                    path_add = dir_name.replace(type_dir_iner + cn + '/' + layer_code + '/out', '')
                                    if path_add.startswith('/'):
                                        path_add = path_add[1:]

                                    dst_file = os.path.join(type_dir_storage, res_code_user['code_user'])
                                    dst_file = os.path.join(dst_file, 'in')
                                    dst_file = os.path.join(dst_file, path_add)
                                    dst_file = os.path.join(dst_file, file_name)
                                    self.move_file(itm, dst_file)

                            # Работа с исходящими файлами vikimini
                            path_search = os.path.join(type_dir_iner, res_code_user['cn'])
                            path_search = os.path.join(path_search, res_code_user['layer_code'])
                            path_search = os.path.join(path_search, 'vikimini')
                            path_search = path_search.replace('\\', '/')
                            self.log_file('Проверка каталога: ' + path_search, terms=1)
                            # проверим существует ли данный каталог если нет, то предупреждение в лог файл
                            if not self.is_exists_folder(path_search):
                                self.log_file(c.m_e_not_exists_folder % path_search + c.t_enter)
                                self.result['result'] = c.plugin_error
                            else:
                                file_list = check_dir_by_path(path_search,
                                                              sub_folders,
                                                              mask_files=mask_files,
                                                              ignore_file='temp')
                            for itm in file_list:
                                # получим каталог в котором находится файл
                                dir_name = os.path.dirname(itm)
                                file_name = os.path.basename(itm)

                                cn = res_code_user['cn']
                                layer_code = res_code_user['layer_code']
                                path_add = dir_name.replace(type_dir_iner + cn + '/' + layer_code + '/', '')
                                if path_add.startswith('/'):
                                    path_add = path_add[1:]

                                # нам нужны только файлы с товарами
                                if file_name.startswith('goods'):
                                    dst_file = os.path.join(type_dir_storage, res_code_user['code_user'])
                                    dst_file = os.path.join(dst_file, path_add)
                                    flag_file = os.path.join(dst_file, 'goods_flag.txt')
                                    dst_file = os.path.join(dst_file, 'goods.txt')
                                    if not self.exists_file(dst_file, add_log=False):
                                        # Если файла нет, то берем переименовываем найденный и создаем файл флага
                                        self.move_file(itm, dst_file)
                                        os.mknod(flag_file)
                                    # Встретили первый подходящий файл, независимо от исхода в коде выше - прерываем
                                    else:
                                        if not self.exists_file(flag_file, add_log=False):
                                            os.mknod(flag_file)
                                    break
        else:
            self.result['result'] = c.plugin_error
            self.log_file(c.m_e_params_is_null)

    def check_relocate(self):
        sns = {}
        if self.sa_token is None:
            self.auth()
        sql_text = 'select id, code_user, cn_dir, cn, layer_code from q_exchange_convert qc'
        converts = self.execute_sql(sql_text, sql_params=[], fetch='many')
        if converts['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения параметров обмена', terms=1)
        else:
            for convert in converts['datalist']:
                email = convert['code_user']
                if convert['cn'] != 'TEST':
                    user = self.user(email)
                    if user is not None and 'node' in user and user['node'] is not None:
                        if 'domain' in user['node'] and user['node']['domain'] is not None:
                            domain = user['node']['domain']
                            # Поищем в списке уже встреченных доменов
                            if domain in sns:
                                sn = sns[domain]
                            else:  # Не нашли
                                # Нормализация имени домена
                                domain_list = domain.split('//')  # Отсекаем начальный http[s]://
                                if len(domain_list) > 1:
                                    correct = domain_list[1]
                                else:
                                    correct = domain_list[0]
                                # Отсекаем все после первого "." и приводим к верхнему регистру
                                sn = str(correct.split('.')[0]).upper()
                                sns[domain] = sn
                            if str(convert['cn']).upper() != sn:
                                # перенесем папку с файлами
                                path_old = os.path.join(convert['cn_dir'], convert['layer_code'])
                                cn_dir_new = str(convert['cn_dir']).replace(convert['cn'], sn)
                                path_new = os.path.join(cn_dir_new, convert['layer_code'])
                                self.move_file(path_old, path_new)
                                # Слой переехал, изменяем данные
                                sql_text = 'UPDATE Q_EXCHANGE_CONVERT set cn_dir=?, cn=? where id=?'
                                sql_params = [cn_dir_new, sn, convert['id']]
                                res = self.execute_sql(sql_text, sql_params=sql_params, fetch='none')
                                if res['status'] == c.kr_sql_error:
                                    self.log_file(res['message'], terms=1)
                        else:
                            self.log_file('Не найден СН для пользователя email=' + email +
                                          ' (возможная причина - пользователь ни разу не авторизовался в сервисе)', terms=1)
                    else:
                        self.log_file('Не найден СН для пользователя %s. СА возвращает пусто.' % email +
                                      'Возможно пользователь удален.', terms=1)

    @classmethod
    def iter(cls):
        cls.iter_num += 1

    @classmethod
    def iter_clear(cls):
        cls.iter_num = 0

    def user(self, email_or_uid):
        """
        Получение параметров пользователя по email или uid
        :param email_or_uid: email или uid
        :return: json
        """

        url = self.sa_url + '/api/user/{email_or_uid}'.format(email_or_uid=email_or_uid)
        headers = {
            'Authorization': 'Token %s' % self.sa_token
        }
        response = requests.get(url, headers=headers, verify=False)  # verify=False Включено на время ДЕБАГА
        if response.status_code == 200:
            return json.loads(response.content)
        else:
            self.log_file('СА вернул ошибку:' + response.content.decode('utf-8').encode('cp1251'))
        return None

    def auth(self):
        """ Авторизация на СА """
        url = self.sa_url + '/api/auth-token'
        headers = {
            'MOBILE-LOGIN': self.sa_username,
            'MOBILE-PASSWORD': self.sa_password
        }
        response = requests.get(url, headers=headers, verify=False)  # verify=False Включено на время ДЕБАГА
        if response.status_code == 200:
            content = json.loads(response.content)
            self.sa_token = content.get('key')
        else:
            raise Exception('Ошибка получения токена авторизации: ' + response.content.decode('utf-8').encode('cp1251'))

    def validate(self, filename, extensions, rules):
        """
        Валидация файла
        @param filename: Полное имя файла
        @param extensions: Допустимые расширения
        @return: Файл прошел валидацию
        @return: Сообщение об ошибке в журнал
        """

        extensions_str = ''
        base_name = os.path.basename(filename)
        ext = base_name.split('.')[-1].upper()
        valid = True
        for rule_extension_key in rules['extensions']:
            rule_extension = rules['extensions'][rule_extension_key]
            extensions_str = extensions_str + rule_extension + '; '
            upper_rule_extension = rule_extension.strip().upper()
            if upper_rule_extension != ext:
                valid = False
        if not valid:
            return False, 'Ошибка валидации файла %s: расширение %s не соответствует возможным (%s)' \
                   % (filename, ext, extensions_str)

        for rule_mask_pos in rules['masks']:
            pattern = str(rules['masks'][rule_mask_pos]['pattern'].decode('cp1251'))
            mask = re.compile(pattern, re.I)
            matches = mask.findall(base_name)
            if matches is None or len(matches) <= 0:
                return False, 'Ошибка валидации файла %s: название файла не соответствует маске %s' \
                       % (filename, rules['masks'][rule_mask_pos]['name'])
        if extensions is not None and len(extensions) > 0:
            valid = True
            for extension in extensions.split(';'):
                extension = extension.strip().upper()
                if extension != ext:
                    valid = False
                    break
            if not valid:
                return False, 'Ошибка валидации файла %s: расширение %s не соответствует возможным (%s)' \
                              % (filename, ext, extensions)
        if ext == 'JSON':
            try:
                with open(filename) as data_file:
                    data = json.load(data_file, encoding='cp1251')
                    keys_str = ''
                    valid = True
                    for rule_key_pos in rules['keys']:
                        rule_key = rules['keys'][rule_key_pos]
                        if (self.find_all_items_json(data, rule_key) == False):
                            keys_str = keys_str + rule_key + '; '
                            valid = False
                    if not valid:
                        return False, 'Ошибка валидации файла %s: отсутствуют следующие ключевые элементы (%s)' \
                               % (filename, keys_str)
            except Exception as e:
                return False, 'Ошибка валидации файла JSON %s: %s' % (filename, e.message)
        elif ext == 'XML':
            try:
                with open(filename) as data_file:
                    tree = Et.parse(data_file)
                    keys_str = ''
                    valid = True
                    for rule_key_pos in rules['keys']:
                        rule_key = rules['keys'][rule_key_pos]
                        root_item = tree.getroot()
                        if (root_item.tag != rule_key):
                            if (len(tree.findall(rule_key)) <= 0):
                                keys_str = keys_str + rule_key + '; '
                                valid = False
                    if not valid:
                        return False, 'Ошибка валидации файла %s: отсутствуют следующие ключевые элементы (%s)' \
                               % (filename, keys_str)
            except Exception as e:
                return False, 'Ошибка валидации файла XML %s: %s' % (filename, e.message)
        return True, ''

    def find_all_items_json(self, obj, key):
        if isinstance(obj, dict):
            if key in obj:
                return True
            for k, v in list(obj.items()):
                if (isinstance(v, dict) or (isinstance(v, list))):
                    if (self.find_all_items_json(v, key) == True):
                        return True
        elif isinstance(obj, list):
            for val in obj:
                if (self.find_all_items_json(val, key) == True):
                    return True
        return False

    @staticmethod
    def to_error(filename, message):
        """
        Перенос файла в каталог с ошибками
        @param filename: Полное имя файла
        @param message: Сообщение в журнал
        """
        # Сперва пишем ошибку
        dir_name = os.path.dirname(filename)
        dir_name = dir_name.replace('out', 'in')
        file_name = os.path.basename(filename)
        # Потенциальный баг, если в имени файла будут точки более одной
        log_name = os.path.join(dir_name, file_name.split('.')[0] + '.log')
        if os.path.exists(log_name):
            log = open(log_name, 'a')
        else:
            log = open(log_name, 'w')
        print(message, file=log)
        log.close()
        # Потом переносим файл
        error_dir_name = os.path.join(dir_name, 'error')
        if not os.path.exists(error_dir_name):
            os.makedirs(error_dir_name)
        dst_name = os.path.join(error_dir_name, file_name)
        shutil.move(filename, dst_name)
