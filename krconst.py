# -*- coding: cp1251-*
"""
    Константы
"""

''' terms '''
kr_term_enter = '\n'
t_enter = '\n'
kr_term_double_enter = '\n\n'
t_double_enter = '\n\n'
kr_term_space = ' '

''' result plugin '''
plugin_ok = 1
plugin_error = 2
plugin_restart = 3
plugin_lock = 4
plugin_lost_connect = 5

''' text type log message '''
log_info = 'INFO'
log_warning = 'WARNING'
log_error = 'ERROR'

''' result execute SQL '''
kr_sql_ok = 1
sql_ok = 1
kr_sql_error = 2
sql_error = 2
kr_sql_lost_connect = 3
sql_lost_connect = 3
sql_timer = 'Время выполнения sql: %(sql)s %(sqlparams)s,  равно = %(delta_date_start_sql)s'
timer_format = 'Время выполнения метода %(name)s равно = %(delta_date_start)s'

''' code status queue '''
kr_status_run = 'R'
kr_status_ok = '1'
kr_status_error = 'E'
kr_status_new = '0'
kr_status_lock = 'L'

''' code status server '''
kr_statusserver_start = 'R'
kr_statusserver_work = 'W'
kr_statusserver_break = 'B'
kr_statusserver_stop = 'S'
kr_statusserver_lostconnect = 'L'
kr_statusserver_close = 'CLOSE'

''' code status config '''
kr_status_config_ok = 1
kr_status_config_error = 0

''' flag '''
kr_flag_stopall = 'SALL'
kr_flag_stopmaintread = 'MTREAD'
kr_flag_logglobal = 'GLOBAL'
kr_flag_logplugin = 'PLUGIN'

''' messages '''
m_e_programm_code_class = 'Ошибка использования класса: %s . ' \
                          'Внутри не обработана ошибка.'
m_e_global_log = 'Ошибка логирования в ядре.'
m_e_plugin_log = 'Ошибка логирования в плагине.'
m_e_log_text = 'Ошибка в тексте лога.'
m_e_file_not_found = 'Файл %s не найден'
kr_message_error_openconnect = 'Error open connect in module: %s'
m_e_unable_to_complete = 'Unable to complete network request to host'
kr_message_error_badflags = 'Bad params FLAG: %s'
m_e_params_is_null = 'Params is null'
kr_message_error_notfoundreport = 'Not found report: %s'
kr_message_error_errorreporttemplate = 'Error in template report: %s'
kr_message_error_errorreportpdf = 'Error create pdf file: %s'
kr_message_error_errorreportpdfprint = 'Error print pdf file: %s'
kr_message_error_errorrimportlib = 'Error importlib: %s'
m_e_delete_file = 'Error delete file: %s'
kr_message_error_errorcopyfile = 'Error copy file: from %s to %s'
kr_message_error_errormovefile = 'Error move file: from %s to %s'
kr_message_error_errorcreatedir = 'Error create dir: %s'
kr_message_error_errorreportimdb = 'Not registered report in DB: %s'
kr_message_error_errorxmlkeyimport = 'Not found key in xml file: %s'
kr_message_error_errorcreatedoc = 'Error create document: %s'
m_e_exec_proc_name = 'Ошибка выполнения процедуры: %s'
kr_message_error_badconfig = 'Bad config xml file'
kr_message_error_updateserverstatus = 'Error update server status.'
m_e_setting_task = 'Ошибка настройки задачи: %s'
kr_message_error_externalfile = 'Error external file: %s'
m_e_i_external_file = 'Ошибка импорта из файла: %s'
m_e_not_exists_folder = 'Не сущеуствует каталога: %s'
m_e_emptytaskparams = 'Пустое значение параметров задания!'
m_e_unpack_file = 'Ошибка распаковки архива: %s'
m_e_server_code_is_none = 'В БД отсутствуют настройки для данного сервера-задач'
m_e_db_none = 'не создан объект подключения к БД'

''' импорт документов '''
m_e_i_conf_file_not_found = 'Файл конфигурации импорта не обнаружен: %s'
m_e_i_doc_conf_file_not_correct = 'Файл конфигурации импорта: %s ' \
                                  'не подходит для обработки файла: %s'
m_w_i_conf_file_def_version = 'Внимание! Файл конфигурации импорта не имеет версию,' \
                              ' будет использована версия обработки 0.0.0.0!'
kr_message_error_importfilenotfound = 'Файл для импорта не обнаружен: %s'
kr_message_error_importfileincorrectconf = 'Файл настройки импорта: %file , имеет неверную структуру: %tag'
m_w_i_doc_not_complete = 'Импорт документа не произошел по следующей причине:'
m_w_i_doc_not_complete_e_msg = 'Ошибка получения причны!'
''' импорт документов '''

''' импорт отчетов '''
m_e_i_report = 'Ошибка импорта отчета обмена.'
''' импорт отчетов '''

''' импорт данных '''
kr_message_error_configimport = 'Ошибка настройки импорта: %s'
kr_message_error_importcustomer = 'Код контрагента: %s'
kr_message_error_importtax = 'Код налога: %s'
kr_message_error_importperiod = 'Код периода: %s'
kr_message_error_importtypeobject = 'Тип объекта: %s'
kr_message_error_importwsetobjbond = 'Ошибка привязки набора: s%  к объектам.'
kr_message_error_importshedule = 'Ошибка создания графиков АЗ: %s'
kr_message_error_importadduserengine = 'Ошибка добавления пользователя в БД Engine: %s'
kr_message_error_importusernull = 'Ошибка получение/добавления пользователя: %s. Наборы не будут привязаны.'
kr_message_error_importwaresgroupnull = 'Ошибка получение группы: %s. Наборы не будут привязаны.'
kr_message_error_importfastenwguser = 'Ошибка привязки группы: %s , к пользователю: %s'
kr_message_error_importgetinstallshop = 'Ошибка получения списка магазинов с установленной системой.'
kr_message_warring_importgetinstallshop = 'Список магазинов пуст с установленной системой.'

m_e_createmail = 'Ошибка создания задания на отправку почты'

m_e_exec_method = 'Ошибка вызова метода: %s.'

# импорт данных
m_e_i_not_need_data_in_file = 'Файл не содержит необходимые данные. Имя файла %s.'

''' импорт категорий'''
m_e_importcategory = 'Код группы объекта: %s'

''' импорт объеков '''
m_e_i_format_obj = 'Формат объекта: %s ,объект: %s'
m_e_i_object = 'Код объекта: %s .'
m_e_i_object_update_wsetbyproducer = 'Ошибка обновления данных для разбивки по производителю.'
m_e_i_object_account = 'Ошибка добавления р/с объекта: %s .'
m_e_i_object_print_data = 'Ошибка импорта print data, объекта: %s'
m_e_i_object_update_status = 'Ошибка обновления статуса объекта: %s.'

''' импорт договоров '''
m_e_i_contracts_update_before = 'Ошибка обновления статуса договоров перед импортом.'
m_e_i_contracts_update_after = 'Ошибка обновления статуса договоров после импорта.'
m_e_i_contracts_update = 'Ошибка обновления статуса договора'
m_e_i_contracts = 'Ошибка импорта договора: %s'
m_e_i_contracts_maker = 'Ошибка импорта привязки договора договора: %s'

''' импорт промо '''
m_e_i_promo = 'Ошибка создания промоакции: %s'
m_e_i_promo_wares = 'Ошибка добавления товара в промоакцию: %s'
m_e_i_promo_fasten_obj = 'Ошибка привязки товара к объекту промоакции: %s'

''' импорт привзки сегмента '''
m_e_i_segment = 'Ошибка импорта привязки сегмента %s к группе %s.'

''' импорт пользователей '''
m_w_ignore_user = 'Пользователь в списке для игнорирования: %s'
m_e_importcheckuserengine = 'Ошибка поиска пользователя по ФИО в БД Engine: %s'
m_e_importcheckuser = 'Ошибка проверки или привязки пользователя к физ лицу: %s'
m_e_importuserenginecnt = 'В БД Engine найдено более одного пользователя с: %s'
m_e_importadduserengine = 'Ошибка добавления пользователя в БД Engine: %s'
m_e_i_set_dolgn = 'Ошибка установки должности: %s'
m_e_i_set_email = 'Ошибка почты: %s'

''' импорт должностей '''
m_w_importdolgn = "Ошибка импорт должности: %s ."

''' импорт форматов '''
m_e_i_format = 'Формат: %s'


''' импорт справочника товар '''
m_e_i_unit = 'Ошибка импорта ед измерения, код : %s'
m_e_i_wgroup = 'Ошибка импорта товарной группы, код: %s'
m_e_i_wares = 'Ошибка импорта товара, код: %s'
m_e_i_wares_unit = 'Ед измерения товара: %s'
m_e_i_wares_barcode = 'ШК товара: %s'
m_e_i_def_view_unit = 'Ошибка установки ед отображения товара: %s'
m_e_i_wares_maker = 'Ошибка импорта производителя товара: %s'
m_e_i_wares_tax = 'Ошибка импорта налоговых ставок товара: %s'
m_e_i_wares_suppliers = 'Ошибка импорта поставщиков товара: %s'
m_e_i_wset_one = 'Ошибка создания наборов. Товар %s'
m_e_i_format_wares = 'Формат объекта: %s ,товар: %s'
m_e_i_wares_type = 'Ошибка импорта типа товара: %s'
m_e_i_wares_mpo = 'Ошибка импорта МПО товара: %s'
m_e_i_wset = 'Ошибка создания наборов.'
m_e_i_wares_season = 'Ошибка импорта сезонности товара: %s'
m_e_i_wares_price_list = 'Ошибка импорта прайсллиста товара: %s'
m_e_g_wgroup_tree = 'Ошибка получения дерева товаров.'
m_e_u_wgroup_external = 'Ошибка обновления external* полей группы: %s.'
m_e_u_wgroup_singularity = 'Ощибка записи свойства товарной группы: %s.'
''' импорт справочника товар '''

''' начало: импорт данных через временные структуры '''
m_e_i_get_tmp_id_get = 'Ошибка получения ID для импорта: %s.'
m_e_i_get_tmp_id_update = 'Ошибка обновления ID для импорта: %s.'
m_e_i_get_tmp_end = 'Ошибка окончания импорта. Ид сессии: %s.'

m_e_i_get_tmp_id_dolgn = 'Ошибка получения должности по ID для импорта: %s.'
m_e_i_get_tmp_id_depart = 'Ошибка получения подразделения по ID для импорта: %s.'
m_e_i_get_tmp_id_employee = 'Ошибка получения сотрудника по ID для импорта: %s.'
m_e_i_get_tmp_id_unit = 'Ошибка получения ед измерения по ID для импорта: %s.'
m_e_i_get_tmp_id_wgroup = 'Ошибка получения группы товара по ID для импорта: %s.'
m_e_i_get_tmp_id_wares = 'Ошибка получения товара по ID для импорта: %s.'
m_e_i_get_tmp_id_waresunit = 'Ошибка получения ед измерения товара по ID для импорта: %s.'

''' конец: импорт данных через временные структуры '''


''' импорт данных '''

''' упаковка временных файлов '''
m_e_pack_dir = 'Ошибка укаповки директории: %s.'
''' упаковка временных файлов '''

''' Печать '''
kr_message_error_printnoparams = 'Нет параметра в задании: %s'
''' Печать '''

''' Проверка каталога '''
m_e_mount_dir = 'Ошибка подключения внешнего ресурса. Команда выполнена неверно: %s'
m_e_umount_dir = 'Ошибка отключения внешнего ресурса. Команда выполнена неверно: %s'
''' Проверка каталога '''

''' Отправка почты '''
m_e_smtp_connect = 'Ошибка подключения к smtp серверу: %s'
m_e_smtp_auth = 'Ошибка авторизации на smtp сервере: %s'
m_e_smtp_send = 'Ошибка отправки письма на smtp сервер: %s'
''' Отправка почты '''

''' прокерка импорта '''
m_e_check_import = 'Ошибка вызова процедуры проверки импорта'
''' прокерка импорта '''

''' импорт рецептов '''
m_e_i_recipe = 'Ошибка импорта рецепта с кодом: %s'
m_e_i_component = 'Ошибка импорта компонета пецепта с кодом: %s'
m_e_i_component_clear = 'Ошибка очистки компонент рецепта с ид: %s'
''' импорт рецептов '''

''' XML '''
m_e_xml_parse_error = 'Ошибка обработки xml файла: %s . XML файл не валидный.'
m_e_xml_parse_str = 'Ошибка обработки xml:' + kr_term_enter + '%s . XML строка не валидна.'
m_e_xml_bad_key = 'В XML файле: %s не найдена необходимая секция: %s'
m_e_xml_not_attr = 'Не передан атрибут'
''' XML '''

kr_message_startserver = 'Start server: %s'
kr_message_startQueueTask = 'Start QueueTask: %s'
kr_message_startbreake = 'Break start'
kr_message_stopbreake = 'Break stop'
m_i_file_create_task_ok = 'File %s add in task ok.' + kr_term_enter

''' import online '''
kr_message_createtaskok = 'Task %s add. ok!'
kr_message_error_firstclear = 'Error first clear doc %s'
kr_message_error_finishtclear = 'Error finish clear doc %s .'
kr_message_error_errorstatus = 'Error status document. Not edit. docid = %s .'
''' import online '''

''' MSSQL '''
kr_message_error_error_MSSQL_DBNone = 'MSSQL DB is None'
kr_message_error_connectstring = 'Ошибка получения connectstring.'
kr_message_error_sqltextfilenotexists = 'Файл %s, содержащий SQL запросы не найден.'
''' MSSQL '''

''' Другие БД '''
m_e_odb_db_none = 'Не передано подключения к БД.'
m_e_odb_exec_sql = 'Ошибка выполнения SQL комманды к БД %(db)s: %(sql)s %(sql_params)s, %(err)s'
m_e_odb_lost_connect = 'Выполнение SQL команды при потерянном коннекте: %s . ' \
                       'Unable to complete network request to host.'
m_e_odb_name_none = 'В конфигурации нет настроек для данного имени: %s'
''' Другие БД '''

''' export scale '''
kr_message_error_unsupportedexport = 'Unsupported type %s export.'
''' export scale '''

''' Version '''
kr_message_warning_version_notsupport = 'Определение версионности не поддерживается'
''' Version '''

''' Импорт чеков '''
m_e_cash_get_cash = 'Ошибка получения списка касс для импорта чеков.'
m_e_cash_get_sales_from_rbs = 'Ошибка получения сумм продаж по чекам из RBS.'
m_e_cash_get_sales_from_cash = 'Ошибка получения сумм продаж по чекам из БД: %s.'
m_e_cash_get_head_from_cash = 'Ошибка получения чеков из %s. За дату %s .'
m_e_cash_get_detail_from_cash = 'Ошибка получения позиций чека из %s. Чек %s .'
m_e_cash_document_save = 'Ошибка сохранения шапки чека. Номер чека: %s .'
m_e_cash_cargo_save = 'Ошибка сохранения позиции чека. Чек номер %s . Товар %s'
m_e_cash_save_status = 'Ошибка обновления реализации по чекам.'
m_e_cash_status = 'Ошибка обновления реализации по чекам.'
m_e_cash_cargo_none = 'Пустые позиции чека: %s'
''' Конец:Импорт чеков '''

''' Для импорта чеков из внешних MSSQL БД '''
kr_message_MSSQL_error_get_sales_from_rbs = 'Ошибка получения сумм продаж по чекам из RBS.'
kr_message_MSSQL_error_get_check_detail_from_mssql = 'Ошибка получения позиций чека из MSSQL. Чек %s .'
kr_message_MSSQL_error_save_document = 'Ошибка сохранения шапки чека. Чек номер %s .'
kr_message_MSSQL_error_save_cargo = 'Ошибка сохранения позиции чека. Чек номер %s . Товар %s'
kr_message_MSSQL_error_save_status = 'Ошибка обновления реализации по чекам.'
''' Для импорта чеков из внешних MSSQL БД '''


''' Для обмена с Logistic '''
kr_message_Logistic_error_get_auto = 'Ошибка получения автомобилей.'
kr_message_Logistic_error_save_auto = 'Ошибка записи автомобиля с номером %s.'
''' Для обмена с Logistic '''


'''
Работа со слоями
'''
m_e_layer_config = 'Неправильная настрока работы со слоями.' \
                   'Сервер-задач не может быть запущен.' \
                   'Режим работы: без слоев.'
m_e_layer_not_db_engine = 'Не возможно подключиться к БД: %s'
m_e_layer_get_layers = 'Ошибка получения слоев по группе: %s'

'''
    API
    Ecwid
'''
m_e_api_get_wgroup = 'Ошибка получения групп'
m_e_api_get_wgroup_continue = 'Ошибка получения продолжения групп'
m_e_api_get_gwares = 'Ошибка получения товаров'
m_e_api_get_gwares_continue = 'Ошибка получения продолжения групп'
