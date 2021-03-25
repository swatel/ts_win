# -*- coding: cp1251-*
"""
    ���������
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
sql_timer = '����� ���������� sql: %(sql)s %(sqlparams)s,  ����� = %(delta_date_start_sql)s'
timer_format = '����� ���������� ������ %(name)s ����� = %(delta_date_start)s'

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
m_e_programm_code_class = '������ ������������� ������: %s . ' \
                          '������ �� ���������� ������.'
m_e_global_log = '������ ����������� � ����.'
m_e_plugin_log = '������ ����������� � �������.'
m_e_log_text = '������ � ������ ����.'
m_e_file_not_found = '���� %s �� ������'
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
m_e_exec_proc_name = '������ ���������� ���������: %s'
kr_message_error_badconfig = 'Bad config xml file'
kr_message_error_updateserverstatus = 'Error update server status.'
m_e_setting_task = '������ ��������� ������: %s'
kr_message_error_externalfile = 'Error external file: %s'
m_e_i_external_file = '������ ������� �� �����: %s'
m_e_not_exists_folder = '�� ����������� ��������: %s'
m_e_emptytaskparams = '������ �������� ���������� �������!'
m_e_unpack_file = '������ ���������� ������: %s'
m_e_server_code_is_none = '� �� ����������� ��������� ��� ������� �������-�����'
m_e_db_none = '�� ������ ������ ����������� � ��'

''' ������ ���������� '''
m_e_i_conf_file_not_found = '���� ������������ ������� �� ���������: %s'
m_e_i_doc_conf_file_not_correct = '���� ������������ �������: %s ' \
                                  '�� �������� ��� ��������� �����: %s'
m_w_i_conf_file_def_version = '��������! ���� ������������ ������� �� ����� ������,' \
                              ' ����� ������������ ������ ��������� 0.0.0.0!'
kr_message_error_importfilenotfound = '���� ��� ������� �� ���������: %s'
kr_message_error_importfileincorrectconf = '���� ��������� �������: %file , ����� �������� ���������: %tag'
m_w_i_doc_not_complete = '������ ��������� �� ��������� �� ��������� �������:'
m_w_i_doc_not_complete_e_msg = '������ ��������� ������!'
''' ������ ���������� '''

''' ������ ������� '''
m_e_i_report = '������ ������� ������ ������.'
''' ������ ������� '''

''' ������ ������ '''
kr_message_error_configimport = '������ ��������� �������: %s'
kr_message_error_importcustomer = '��� �����������: %s'
kr_message_error_importtax = '��� ������: %s'
kr_message_error_importperiod = '��� �������: %s'
kr_message_error_importtypeobject = '��� �������: %s'
kr_message_error_importwsetobjbond = '������ �������� ������: s%  � ��������.'
kr_message_error_importshedule = '������ �������� �������� ��: %s'
kr_message_error_importadduserengine = '������ ���������� ������������ � �� Engine: %s'
kr_message_error_importusernull = '������ ���������/���������� ������������: %s. ������ �� ����� ���������.'
kr_message_error_importwaresgroupnull = '������ ��������� ������: %s. ������ �� ����� ���������.'
kr_message_error_importfastenwguser = '������ �������� ������: %s , � ������������: %s'
kr_message_error_importgetinstallshop = '������ ��������� ������ ��������� � ������������� ��������.'
kr_message_warring_importgetinstallshop = '������ ��������� ���� � ������������� ��������.'

m_e_createmail = '������ �������� ������� �� �������� �����'

m_e_exec_method = '������ ������ ������: %s.'

# ������ ������
m_e_i_not_need_data_in_file = '���� �� �������� ����������� ������. ��� ����� %s.'

''' ������ ���������'''
m_e_importcategory = '��� ������ �������: %s'

''' ������ ������� '''
m_e_i_format_obj = '������ �������: %s ,������: %s'
m_e_i_object = '��� �������: %s .'
m_e_i_object_update_wsetbyproducer = '������ ���������� ������ ��� �������� �� �������������.'
m_e_i_object_account = '������ ���������� �/� �������: %s .'
m_e_i_object_print_data = '������ ������� print data, �������: %s'
m_e_i_object_update_status = '������ ���������� ������� �������: %s.'

''' ������ ��������� '''
m_e_i_contracts_update_before = '������ ���������� ������� ��������� ����� ��������.'
m_e_i_contracts_update_after = '������ ���������� ������� ��������� ����� �������.'
m_e_i_contracts_update = '������ ���������� ������� ��������'
m_e_i_contracts = '������ ������� ��������: %s'
m_e_i_contracts_maker = '������ ������� �������� �������� ��������: %s'

''' ������ ����� '''
m_e_i_promo = '������ �������� ����������: %s'
m_e_i_promo_wares = '������ ���������� ������ � ����������: %s'
m_e_i_promo_fasten_obj = '������ �������� ������ � ������� ����������: %s'

''' ������ ������� �������� '''
m_e_i_segment = '������ ������� �������� �������� %s � ������ %s.'

''' ������ ������������� '''
m_w_ignore_user = '������������ � ������ ��� �������������: %s'
m_e_importcheckuserengine = '������ ������ ������������ �� ��� � �� Engine: %s'
m_e_importcheckuser = '������ �������� ��� �������� ������������ � ��� ����: %s'
m_e_importuserenginecnt = '� �� Engine ������� ����� ������ ������������ �: %s'
m_e_importadduserengine = '������ ���������� ������������ � �� Engine: %s'
m_e_i_set_dolgn = '������ ��������� ���������: %s'
m_e_i_set_email = '������ �����: %s'

''' ������ ���������� '''
m_w_importdolgn = "������ ������ ���������: %s ."

''' ������ �������� '''
m_e_i_format = '������: %s'


''' ������ ����������� ����� '''
m_e_i_unit = '������ ������� �� ���������, ��� : %s'
m_e_i_wgroup = '������ ������� �������� ������, ���: %s'
m_e_i_wares = '������ ������� ������, ���: %s'
m_e_i_wares_unit = '�� ��������� ������: %s'
m_e_i_wares_barcode = '�� ������: %s'
m_e_i_def_view_unit = '������ ��������� �� ����������� ������: %s'
m_e_i_wares_maker = '������ ������� ������������� ������: %s'
m_e_i_wares_tax = '������ ������� ��������� ������ ������: %s'
m_e_i_wares_suppliers = '������ ������� ����������� ������: %s'
m_e_i_wset_one = '������ �������� �������. ����� %s'
m_e_i_format_wares = '������ �������: %s ,�����: %s'
m_e_i_wares_type = '������ ������� ���� ������: %s'
m_e_i_wares_mpo = '������ ������� ��� ������: %s'
m_e_i_wset = '������ �������� �������.'
m_e_i_wares_season = '������ ������� ���������� ������: %s'
m_e_i_wares_price_list = '������ ������� ����������� ������: %s'
m_e_g_wgroup_tree = '������ ��������� ������ �������.'
m_e_u_wgroup_external = '������ ���������� external* ����� ������: %s.'
m_e_u_wgroup_singularity = '������ ������ �������� �������� ������: %s.'
''' ������ ����������� ����� '''

''' ������: ������ ������ ����� ��������� ��������� '''
m_e_i_get_tmp_id_get = '������ ��������� ID ��� �������: %s.'
m_e_i_get_tmp_id_update = '������ ���������� ID ��� �������: %s.'
m_e_i_get_tmp_end = '������ ��������� �������. �� ������: %s.'

m_e_i_get_tmp_id_dolgn = '������ ��������� ��������� �� ID ��� �������: %s.'
m_e_i_get_tmp_id_depart = '������ ��������� ������������� �� ID ��� �������: %s.'
m_e_i_get_tmp_id_employee = '������ ��������� ���������� �� ID ��� �������: %s.'
m_e_i_get_tmp_id_unit = '������ ��������� �� ��������� �� ID ��� �������: %s.'
m_e_i_get_tmp_id_wgroup = '������ ��������� ������ ������ �� ID ��� �������: %s.'
m_e_i_get_tmp_id_wares = '������ ��������� ������ �� ID ��� �������: %s.'
m_e_i_get_tmp_id_waresunit = '������ ��������� �� ��������� ������ �� ID ��� �������: %s.'

''' �����: ������ ������ ����� ��������� ��������� '''


''' ������ ������ '''

''' �������� ��������� ������ '''
m_e_pack_dir = '������ �������� ����������: %s.'
''' �������� ��������� ������ '''

''' ������ '''
kr_message_error_printnoparams = '��� ��������� � �������: %s'
''' ������ '''

''' �������� �������� '''
m_e_mount_dir = '������ ����������� �������� �������. ������� ��������� �������: %s'
m_e_umount_dir = '������ ���������� �������� �������. ������� ��������� �������: %s'
''' �������� �������� '''

''' �������� ����� '''
m_e_smtp_connect = '������ ����������� � smtp �������: %s'
m_e_smtp_auth = '������ ����������� �� smtp �������: %s'
m_e_smtp_send = '������ �������� ������ �� smtp ������: %s'
''' �������� ����� '''

''' �������� ������� '''
m_e_check_import = '������ ������ ��������� �������� �������'
''' �������� ������� '''

''' ������ �������� '''
m_e_i_recipe = '������ ������� ������� � �����: %s'
m_e_i_component = '������ ������� ��������� ������� � �����: %s'
m_e_i_component_clear = '������ ������� ��������� ������� � ��: %s'
''' ������ �������� '''

''' XML '''
m_e_xml_parse_error = '������ ��������� xml �����: %s . XML ���� �� ��������.'
m_e_xml_parse_str = '������ ��������� xml:' + kr_term_enter + '%s . XML ������ �� �������.'
m_e_xml_bad_key = '� XML �����: %s �� ������� ����������� ������: %s'
m_e_xml_not_attr = '�� ������� �������'
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
kr_message_error_connectstring = '������ ��������� connectstring.'
kr_message_error_sqltextfilenotexists = '���� %s, ���������� SQL ������� �� ������.'
''' MSSQL '''

''' ������ �� '''
m_e_odb_db_none = '�� �������� ����������� � ��.'
m_e_odb_exec_sql = '������ ���������� SQL �������� � �� %(db)s: %(sql)s %(sql_params)s, %(err)s'
m_e_odb_lost_connect = '���������� SQL ������� ��� ���������� ��������: %s . ' \
                       'Unable to complete network request to host.'
m_e_odb_name_none = '� ������������ ��� �������� ��� ������� �����: %s'
''' ������ �� '''

''' export scale '''
kr_message_error_unsupportedexport = 'Unsupported type %s export.'
''' export scale '''

''' Version '''
kr_message_warning_version_notsupport = '����������� ������������ �� ��������������'
''' Version '''

''' ������ ����� '''
m_e_cash_get_cash = '������ ��������� ������ ���� ��� ������� �����.'
m_e_cash_get_sales_from_rbs = '������ ��������� ���� ������ �� ����� �� RBS.'
m_e_cash_get_sales_from_cash = '������ ��������� ���� ������ �� ����� �� ��: %s.'
m_e_cash_get_head_from_cash = '������ ��������� ����� �� %s. �� ���� %s .'
m_e_cash_get_detail_from_cash = '������ ��������� ������� ���� �� %s. ��� %s .'
m_e_cash_document_save = '������ ���������� ����� ����. ����� ����: %s .'
m_e_cash_cargo_save = '������ ���������� ������� ����. ��� ����� %s . ����� %s'
m_e_cash_save_status = '������ ���������� ���������� �� �����.'
m_e_cash_status = '������ ���������� ���������� �� �����.'
m_e_cash_cargo_none = '������ ������� ����: %s'
''' �����:������ ����� '''

''' ��� ������� ����� �� ������� MSSQL �� '''
kr_message_MSSQL_error_get_sales_from_rbs = '������ ��������� ���� ������ �� ����� �� RBS.'
kr_message_MSSQL_error_get_check_detail_from_mssql = '������ ��������� ������� ���� �� MSSQL. ��� %s .'
kr_message_MSSQL_error_save_document = '������ ���������� ����� ����. ��� ����� %s .'
kr_message_MSSQL_error_save_cargo = '������ ���������� ������� ����. ��� ����� %s . ����� %s'
kr_message_MSSQL_error_save_status = '������ ���������� ���������� �� �����.'
''' ��� ������� ����� �� ������� MSSQL �� '''


''' ��� ������ � Logistic '''
kr_message_Logistic_error_get_auto = '������ ��������� �����������.'
kr_message_Logistic_error_save_auto = '������ ������ ���������� � ������� %s.'
''' ��� ������ � Logistic '''


'''
������ �� ������
'''
m_e_layer_config = '������������ �������� ������ �� ������.' \
                   '������-����� �� ����� ���� �������.' \
                   '����� ������: ��� �����.'
m_e_layer_not_db_engine = '�� �������� ������������ � ��: %s'
m_e_layer_get_layers = '������ ��������� ����� �� ������: %s'

'''
    API
    Ecwid
'''
m_e_api_get_wgroup = '������ ��������� �����'
m_e_api_get_wgroup_continue = '������ ��������� ����������� �����'
m_e_api_get_gwares = '������ ��������� �������'
m_e_api_get_gwares_continue = '������ ��������� ����������� �����'
