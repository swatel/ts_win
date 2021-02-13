# -*- coding: utf-8 -*-
import re
import hashlib
import requests
import json
import glob
import os
import ntpath
import shutil

import krconst as c
import kconfig as conf
import queue_db as db


class BankClient(object):
    sa_url = None  # Url сервера авторизации
    sa_username = None  # Пользователь сервера авторизации
    sa_password = None  # Пароль сервера авторизации
    sa_token = None  # Токен сервера авторизации
    mask_files = None  # Маска имен файлов для поиска
    doc_types = None  # Типы документов для загрузки
    ignore_inn = None  # Список ИНН плательщиков, которых игнорировать
    recipients = None  # Список ИНН получателей
    regex_email = re.compile(r"([0-9a-zA-Z][a-zA-Z0-9._-]*@[a-zA-Z0-9._-]+\.[a-zA-Z]{2,6})", re.IGNORECASE)
    regex_uid = re.compile(r"[лЛ][\\\\/]?[сС][чЧ]*\s*[№]*\s*([0-9]{1,9})", re.IGNORECASE)
    # regex_num = re.compile(r"^.*?([0-9]-[0-9]{8}).*$")
    regex_email_or_uid = re.compile(r"^.*?\[(.*?)\].*$")
    prefix = 'kb'  # Префикс для формируемых файлов
    extention = '.txt'  # Расширение для формируемых файлов

    def __init__(self, options):
        if 'sa_url' in options:
            self.sa_url = options['sa_url']
        if 'sa_username' in options:
            self.sa_username = options['sa_username']
        if 'sa_password' in options:
            self.sa_password = options['sa_password']
        if 'doc_types' in options:
            self.doc_types = frozenset(options['doc_types'])
        if 'ignore_inn' in options:
            self.ignore_inn = frozenset(options['ignore_inn'])
        if 'recipients' in options:
            self.recipients = frozenset(options['recipients'])
        if 'regex_email' in options:
            self.regex_email = re.compile(options['regex_email'], re.IGNORECASE)
        if 'regex_uid' in options:
            self.regex_uid = re.compile(options['regex_uid'], re.IGNORECASE)
        # if 'regex_num' in options:
        #     self.regex_num = re.compile(options['regex_num'], re.IGNORECASE)
        self.mask_files = options['mask_files'] if 'mask_files' in options else '*.txt'
        # Папка для сортировки
        self.sort_dir = self.__init_dir(options, 'sort_dir', './sort')
        # Папка для результатов
        self.out_dir = self.__init_dir(options, 'out_dir', './out')
        # Папка для ошибок
        self.error_dir = self.__init_dir(options, 'error_dir', './error')
        # Папка входящие, внутри доменной папки
        if 'domain_in_dir' in options:
            self.domain_in_dir = options['domain_in_dir']

    def sort(self, parent_class, check_dir):
        """
        Поиск файлов в каталоге и передача их дальше в процедуру разбора
        :param parent_class: ссылка на родительский класс для логирония
        :param check_dir: каталог, содержащий файлы
        :return: boolean
        """
        # Параметры авторизации
        if self.sa_url is None:
            raise Exception('SA url error')
        if self.sa_username is None:
            raise Exception('SA username error')
        if self.sa_password is None:
            raise Exception('SA password error')
        path = check_dir.replace('\\', '/')
        # Поиск файлов в каталоге по маске
        for mask in self.mask_files.split(','):
            file_list = sorted(glob.glob(path + '/' + mask), key=os.path.getmtime)
            for filename in file_list:
                filename = filename.replace('\\', '/')
                basename = ntpath.basename(filename)
                try:
                    if self.parse(filename):
                        # Все прошло хорошо - перемещаем в папку результатов
                        shutil.move(filename, self.out_dir + '/' + basename)
                except Exception as exc:
                    parent_class.log_file(str(exc), terms=1)
                    # Ошибка - перемещаем в папку ошибок
                    self.__to_error(filename, 'Непредвиденная ошибка разбора файла')
            # Сортируем уже после разбора всех файлов
            self.__sort(parent_class)
        return True

    def process_import(self, parent_class, engine_conf, db_engine):
        """
        Импорт файлов КБ в базу
        :param parent_class: BasePlugin
        :param db_engine: Engine DB connection
        :return:
        """
        # подключение к БД Engine
        if parent_class is None or db_engine is None:
            raise Exception('KB process import params error')
        path = self.sort_dir.replace('\\', '/')
        # Поиск файлов
        for mask in self.mask_files.split(','):
            file_list = sorted(glob.glob(path + '/' + mask), key=os.path.getmtime)
            for filename in file_list:
                parent_class.log_file('Обрабатываем файл:' + filename.encode('cp1251'), terms=1)
                basename = ntpath.basename(filename)
                # Поиск uid в имени файла
                matches = self.regex_email_or_uid.match(basename)
                if matches is not None and matches.group(1) != '':
                    uid = matches.group(1)
                else:
                    uid = None
                # Здесь определяем слой
                db_conn = None
                sql_text = '''select first 1 l.code, l.disabled
                                from engine_users u
                                left join engine_layers l on u.layer_id = l.layer_id
                               where sa_uid = ?'''
                sql_params = [uid]
                res = parent_class.execute_sql(sql_text,
                                               sql_params=sql_params,
                                               db_local=db_engine,
                                               fetch='one'
                                               )
                if res['status'] == c.kr_sql_error:
                    parent_class.log_file('Ошибка получения слоя по uid ' + str(uid), terms=1)
                    self.__to_error(filename, 'Ошибка получения слоя по uid ' + str(uid))
                    disabled = False
                else:
                    layer_code = res['datalist']['code']
                    disabled = res['datalist']['disabled'] == 1
                    if layer_code:
                        parent_class.log_file('Подключаемся слою:' + layer_code, terms=1)
                        # получаем подключение к БД слоя
                        k_conf = conf.KConfig(layer_code)
                        k_conf.get_os_version()
                        k_conf.get_config_file()
                        k_conf.get_config(layer_code, engine_conf)
                        db_conn = db.QueryDB(k_conf)

                if db_conn is None:
                    parent_class.log_file('Ошибка подключения к слою ' + layer_code, terms=1)
                    self.__to_error(filename, 'Ошибка подключения к слою ' + layer_code)
                else:
                    # Чтение из файла
                    parent_class.log_file('Подключение прошло успешно', terms=1)
                    f = open(filename, mode='r')
                    line = f.readline().strip()
                    # Проверям первую строку на корректность
                    self.__check_first_line(line, filename)
                    in_doc_section = False
                    doc = {}
                    # Разбор строк файла
                    while line:
                        if line != '':
                            try:
                                key, value = line.split('=', 1)
                            except ValueError:
                                key = line
                                value = None

                            if not in_doc_section:
                                # Зашли уже в новый документ?
                                in_doc_section = key == 'СекцияДокумент' and (self.doc_types is None or value in self.doc_types)
                            if in_doc_section:
                                # Пишем в документ
                                doc[key] = value
                                in_doc_section = key != 'КонецДокумента'
                                if not in_doc_section:
                                    break
                        line = f.readline().strip()
                    f.close()

                    # Поиск номера счета в назначении платежа - не актуально, только в документе Номер
                    # destination = doc['НазначениеПлатежа']
                    # matches = self.regex_num.match(destination)
                    # if matches is not None:
                    #     num = matches.group(1)
                    if 'Номер' in doc:
                        num = doc['Номер']
                    else:
                        num = None
                    """ 15.12.2015
                    SUMMA integer, сумма пополнения
                    ADATETIME timestamp, дата пополнения по счету
                    "TYPE" smallint, (-1,1) где -1 снятие с баланса, 1 пополнение баланса(в твоем случае 1)
                    TYPEACTION varchar(20), признак отруда пришло действие(например робокасса, крон) тут что-то придумай :)
                    COUNTLICENSE integer, количество лицензий, в твоем случае null
                    NUMMONTH double precision, за сколько месяцев, в твоем случае null
                    NUMACT varchar(20))
                    """
                    try:
                        amount = int(float(doc['Сумма']))
                    except ValueError:
                        parent_class.log_file('Ошибка преобразования суммы', terms=1)
                        # Файл сбрасываем в ошибки
                        self.__to_error(filename, 'Ошибка преобразования суммы')
                        # Прерываем работу над файлом
                        continue
                    date = doc['Дата']  # TODO Разобраться с преобразованием типа
                    type_in = 1
                    action = 'ClientBank'
                    lic = None
                    month = None
                    sql_params = [amount, date, type_in, action, lic, month, num]
                    # Здесь записываем данные в слой
                    sql_text = 'execute procedure MY_UPDATE_BALANCE(?,?,?,?,?,?,?)'
                    res = parent_class.execute_sql(sql_text, sql_params=sql_params, db_local=db_conn)
                    if res['status'] == c.kr_sql_error:
                        parent_class.log_file(res['message'], terms=1)
                        self.__to_error(filename, res['message'])
                    else:
                        # процедура проверки списания с баланса
                        if disabled:
                            parent_class.log_file('Слой заблокирован, списание с баланса отложено', terms=1)
                            self.__to_error(filename, 'Слой заблокирован, списание с баланса отложено')
                        else:
                            parent_class.log_file('Проверяем списание баланса...', terms=1)
                            sql_text = 'execute procedure CRON_UPDATE_TARIF'
                            res = parent_class.execute_sql(sql_text,
                                                           sql_params=[],
                                                           db_local=db_conn,
                                                           fetch='none')
                            if res['status'] == c.kr_sql_error:
                                parent_class.log_file(res['message'], terms=1)
                                self.__to_error(filename, res['message'])
                            else:
                                parent_class.log_file('Проверка прошла успешно', terms=1)
                                self.__to_out(filename)
                                # проставление номеров актов, тут уже не важно будет ли ошибка
                                parent_class.log_file('Нумеруем акты...', terms=1)
                                sql_text = 'SELECT bh.BALANCEHISTORYID ' \
                                           '  FROM BALANCE_HISTORY bh ' \
                                           ' WHERE bh.TYPE = -1 AND bh.NUMACT is NULL'
                                res = parent_class.execute_sql(sql_text,
                                                               sql_params=[],
                                                               db_local=db_conn,
                                                               fetch='many')
                                if res['status'] == c.kr_sql_error:
                                    parent_class.log_file(res['message'], terms=1)
                                    self.__to_error(filename, res['message'])
                                else:
                                    balancehistory = res['datalist']
                                    for itm in balancehistory:
                                        # получим новый номер акта
                                        sql_text = 'select act_num from lb_get_act_num'
                                        res_num = parent_class.execute_sql(sql_text,
                                                                           sql_params=[],
                                                                           db_local=db_engine,
                                                                           fetch='one')
                                        if res_num['status'] == c.kr_sql_error:
                                            parent_class.log_file('Ошибка получения нового номера акта', terms=1)
                                            parent_class.log_file(res_num['message'], terms=1)
                                        else:
                                            sql_text = 'UPDATE BALANCE_HISTORY SET NUMACT=? WHERE BALANCEHISTORYID=?'
                                            sql_params = [res_num['datalist']['act_num'], itm['BALANCEHISTORYID']]
                                            res = parent_class.execute_sql(sql_text,
                                                                           sql_params=sql_params,
                                                                           db_local=db_conn,
                                                                           fetch='none')
                                            if res['status'] == c.kr_sql_error:
                                                message = 'Ошибка обновления номера акта: ' + str(itm['BALANCEHISTORYID'])
                                                parent_class.log_file(message, terms=1)
                                                parent_class.log_file(res['message'], terms=1)
                                                self.__to_error(filename, res['message'])

    @staticmethod
    def __init_dir(options, key, default):
        if key in options and options[key] != '':
            value = options[key]
        else:
            value = default
        value = os.path.abspath(value).replace('\\', '/')
        if not os.path.exists(value + '/'):
            os.makedirs(value)
        return value

    @staticmethod
    def __check_first_line(line, filename):
        if not line or line != '1CClientBankExchange':
            raise Exception('File %s is not 1CClientBankExchange' % filename)

    def parse(self, filename):
        failed = False
        # ordered_keys для сборки файла в порядке оригинального файла
        header = {'ordered_keys': []}
        f = open(filename, mode='r')
        line = f.readline().strip()
        self.__check_first_line(line, filename)

        in_header_section = True
        in_doc_section = False
        while line:
            if line != '':
                try:
                    key, value = line.split('=', 1)
                except ValueError:
                    key = line
                    value = None

                if in_header_section:
                    # Проверка, может заголовок уже закончился?
                    in_header_section = key != 'СекцияДокумент' and key != 'СекцияРасчСчет'

                if in_header_section:
                    # Если не закончился - продолжаем писать в него
                    header['ordered_keys'].append(key)
                    header[key] = value
                else:
                    if not in_doc_section:
                        # Зашли уже в новый документ?
                        in_doc_section = key == 'СекцияДокумент'
                        if in_doc_section:
                            # Сбросим в значение по умолчанию
                            # ordered_keys для сборки файла в порядке оригинального файла
                            doc = {'ordered_keys': []}
                    if in_doc_section:
                        # Пишем в документ
                        doc['ordered_keys'].append(key)
                        doc[key] = value
                        in_doc_section = key != 'КонецДокумента'
                        if not in_doc_section:
                            self.save(header, doc)
            line = f.readline().strip()
        f.close()
        return not failed

    def save(self, header, doc):
        ignore = False
        error = ''
        # Фильтрация по ИНН плательщика
        if self.ignore_inn is not None:
            payer_inn = doc['ПлательщикИНН']
            if payer_inn in self.ignore_inn:
                error = 'Пропуск плательщика по ИНН ' + payer_inn
                ignore = True
        # Фильтрация по ИНН получателя
        if self.recipients is not None:
            recipient_inn = doc['ПолучательИНН']
            if recipient_inn not in self.recipients:
                error = 'Пропуск получателя по ИНН ' + recipient_inn
                ignore = True
        # Фильтрация по типу документа
        doc_type = doc['СекцияДокумент']
        if self.doc_types is None or doc_type not in self.doc_types:
            error = 'Пропуск документа по типу ' + doc_type
            ignore = True
        inn = doc['ПлательщикИНН']
        number = doc['Номер']
        try:
            date = doc['Дата'].split('.')
            date = date[2]+'-'+date[1]+'-'+date[0]
        except ValueError:
            date = doc['Дата']
        destination = doc['НазначениеПлатежа']
        email = ''
        uid = ''
        if not ignore:
            # Поиск uid в назначении платежа
            # print destination
            matches = self.regex_uid.findall(destination)
            if matches is not None and len(matches) > 0:
                if len(matches) == 1:
                    uid = matches[0].lstrip('0')
                else:
                    error += 'Назначение платежа содержит более одного л.с.'
            # Поиск email в назначении платежа
            matches = self.regex_email.findall(destination)
            if matches is not None and len(matches) > 0:
                if len(matches) == 1:
                    email = matches[0]
                else:
                    error += 'Назначение платежа содержит более одного email'
        # Для контроля uid и email будут сопоставляться при сортировке
        if len(uid) > 0 and len(email) > 0:
            email_or_uid = uid + ';' + email
        elif len(uid) > 0:
            email_or_uid = uid
        elif len(email) > 0:
            email_or_uid = email
        else:
            email_or_uid = ''
        # Хеш от назначения платежа для "уникальной" идентификации документа
        m = hashlib.md5()
        m.update(destination)
        filename = self.prefix + '_' + date + '_' + inn + '_' + number + '_[' + email_or_uid + ']_' + \
                   m.hexdigest() + self.extention
        f = open(self.sort_dir + '/' + filename, mode='w')
        # Запись в файл
        for key in header['ordered_keys']:
            line = key
            if header[key] is not None:
                line += '='+header[key]
            f.write(line+"\n")
        for key in doc['ordered_keys']:
            line = key
            if doc[key] is not None:
                line += '='+doc[key]
            f.write(line+"\n")

        f.write('КонецФайла'+"\n")
        f.close()
        if len(error) > 0:
            self.__to_error(self.sort_dir + '/' + filename, error)
        return True

    def __to_error(self, filename, message=None):
        """
        Переместить файл в каталог ошибок
        :param filename: Имя файла
        """
        basename = ntpath.basename(filename)
        shutil.move(filename, self.error_dir + '/' + basename)
        # Если есть сообщение с ошибкой - запишем в файл
        if message is not None:
            f = open(self.error_dir + '/' + basename, mode='a')
            f.write('ОшибкаРазбора=' + message + "\n")
            f.close()

    def __to_out(self, filename):
        """
        Переместить файл в каталог результатов
        :param filename: Имя файла
        """
        basename = ntpath.basename(filename)
        shutil.move(filename, self.out_dir + '/' + basename)

    def __sort(self, parent_class):
        """
        Сортировка документов для СН
        :param parent_class: ссылка на родительский класс для логирония
        :return: boolean
        """
        token = self.auth()
        sn_correct = {}
        # Получение списка файлов в папке для сортировки
        file_list = sorted(glob.glob(self.sort_dir + '/' + self.prefix + '*' + self.extention), key=os.path.getmtime)
        for filename in file_list:
            filename = filename.replace('\\', '/')
            basename = ntpath.basename(filename)
            # Пытаемся получить email или uid из имени файла
            matches = self.regex_email_or_uid.match(basename)
            if matches is not None and matches.group(1) != '':
                email_or_uid = matches.group(1)
                uid_from_file = ''
                email_from_file = ''
                msg_error = None
                try:
                    if ';' in email_or_uid:
                        # Если в файле есть оба идентификатора -- сопоставить их позднее
                        uid_from_file, email_from_file = email_or_uid.split(';')
                        user = self.user(token, uid_from_file)
                    else:
                        # Получение параметров с сервера
                        user = self.user(token, email_or_uid)
                    if user['error']:
                        msg_error = user['message']
                        user = None
                    else:
                        user = user['result']
                except Exception as exc:
                    parent_class.log_file('Сработало исключение:')
                    parent_class.log_file(str(exc))
                    msg_error = str(exc)
                    user = None
                if user is None:
                    # Ошибка или пользователь не найден
                    msg = 'Пользователь %s не найден' % email_or_uid.encode('cp1251')
                    if msg_error:
                        msg += '. ' + msg_error
                    self.__to_error(filename, msg)
                    parent_class.log_file(msg)
                    parent_class.log_file('Имя файла:' + filename.encode('cp1251') + '\n')
                    parent_class.log_file('Токен:' + token.encode('cp1251') + '\n')
                    parent_class.log_file('Параметр по которму определям:' + email_or_uid.encode('cp1251') + '\n')
                elif not user['is_active']:
                    # Пользователь заблокирован
                    msg = 'Пользователь %s заблокирован' % email_or_uid.encode('cp1251')
                    self.__to_error(filename, msg)
                else:  # Нашли, сортируем
                    uid = user['uid']
                    email = user['email']
                    # Если в файле есть оба идентификатора -- сопоставить их
                    if uid_from_file != '' and email_from_file != '':
                        if uid_from_file != str(uid) or email.lower() != email_from_file.lower():
                            print(uid_from_file, '=', uid)
                            print(email_from_file, '=', email)
                            self.__to_error(filename, 'Найдено несоответствие uid и email в документе')
                            continue
                    # Все ок - продолжаем
                    if email_or_uid != uid:
                        # Если в имени файла был email или uid с ведущими нулями - заменим на нормальный uid
                        basename = basename.replace(email_or_uid, str(uid))
                    # Получили домен с сервера

                    if 'node' in user and user['node'] is not None:
                        if 'domain' in user['node'] and user['node']['domain'] is not None:
                            domain = user['node']['domain']
                            # Поищем в списке уже встреченных доменов
                            if domain not in sn_correct:  # Не нашли
                                # Нормализация имени домена
                                domain_list = domain.split('//')  # Отсекаем начальный http[s]://
                                if len(domain_list) > 1:
                                    correct = domain_list[1]
                                else:
                                    correct = domain_list[0]
                                correct = correct.split('/')[0]  # Отсекаем все после первого "/"
                                sn_correct[domain] = correct
                            # Получим откорректированное имя домена для использования в качестве имени папки
                            domain = sn_correct[domain]
                            # Создадим папку, если ее нет
                            temp_path = os.path.join(self.out_dir, domain)
                            temp_path = os.path.join(temp_path.encode('cp1251'), self.domain_in_dir)
                            temp_path = temp_path.decode('cp1251')

                            if not os.path.exists(temp_path):
                                os.makedirs(temp_path)
                            # Переместим файл в папку нужного СН
                            shutil.move(filename, os.path.join(temp_path, basename))
                        else:
                            self.__to_error(filename, 'Не найден СН для пользователя uid=' + uid +
                                            ' (возможная причина - пользователь ни разу не авторизовался в сервисе)')
                    else:
                        self.__to_error(filename, 'Не найден СН для пользователя. СА возвращает пусто.')
            else:
                self.__to_error(filename, 'В назначении платежа не найдено реквизитов для поиска слоя')
        return True

    def user(self, token=None, email_or_uid=None):
        """
        Получение параметров пользователя по email или uid
        :param token: Токен авторизации
        :param email_or_uid: email или uid
        :return: json
        """

        result = dict()
        result['error'] = False
        result['result'] = None
        result['message'] = ''

        if token is None or email_or_uid is None:
            result['error'] = True
            if token is None:
                result['message'] = 'Не был получен токее с СА. '
            if email_or_uid is None:
                result['message'] = 'В основании платежа нет информации для определения слоя. '
            return result
        url = self.sa_url + '/api/user/{email_or_uid}'
        headers = {
            'Authorization': 'Token %s' % token
        }
        response = requests.get(url.format(email_or_uid=email_or_uid), headers=headers, verify=False)  # Включено на время ДЕБАГА
        # response = requests.get(url.format(email_or_uid=email_or_uid), headers=headers)  # TODO Использовать на продакшене
        if response.status_code == 200:
            content = json.loads(response.content)
            result['result'] = content
        else:
            result['error'] = True
            result['message'] = 'СА вернул ошибку:' + response.content.decode('utf-8').encode('cp1251')
        return result

    def auth(self):
        """
        Авторизация на СА
        :return: Токен авторизации
        """
        url = self.sa_url + '/api/auth-token'
        headers = {
            'MOBILE-LOGIN': self.sa_username,
            'MOBILE-PASSWORD': self.sa_password
        }
        response = requests.get(url, headers=headers, verify=False)  # Включено на время ДЕБАГА
        # response = requests.get(url, headers=headers)  # TODO Использовать на продакшене
        if response.status_code == 200:
            content = json.loads(response.content)
            token = content.get('key')
            return token
        else:
            raise Exception('Auth token error')
