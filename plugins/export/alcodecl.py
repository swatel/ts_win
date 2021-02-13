# -*- coding: utf-8 -*-

"""
    Модуль формирования алкоделарации
"""

import os
import json
import uuid
import configparser
from xml.dom import minidom
from xml.etree.cElementTree import Element, SubElement, tostring

import krconst as c
import queue_db as db
import BasePlugin as Bp
import kconfig as conf
from rbsqutils import str_to_bool_int
from rbsqutils import formatMxDateTime
from rbsqutils import decodeUStr


class Plugin(Bp.BasePlugin):
    """
    Класс предварительной обработки файлов
    """

    date = None
    file_name_error = None
    errors = []
    db_conn = None
    path_file = None
    layer_code = None

    def run(self):
        """
            Запуск плагина на исполнение
        """

        # получаем параметры задания
        self.errors = []
        self.layer_code = self.parser_xml(self.queueparamsxml, 'layer_code')
        self.path_file = self.parser_xml(self.queueparamsxml, 'path_file')
        form = self.parser_xml(self.queueparamsxml, 'form')
        org_id = self.parser_xml(self.queueparamsxml, 'org_id')
        year = self.parser_xml(self.queueparamsxml, 'year')
        period = self.parser_xml(self.queueparamsxml, 'period')
        ignore_error = self.parser_xml(self.queueparamsxml, 'ignore_error')
        if ignore_error == 'on':
            ignore_error = True
        else:
            ignore_error = False

        # получим подключение к БД Engine
        self.log_file('Подклчение к Engine', terms=1)
        engine_conf = conf.KConfig('ENGINE_LITEBOX')
        engine_conf.get_config_file()
        engine_conf.get_config_layer()
        engine_conf.get_config()

        # подключимся к слою
        k_conf = conf.KConfig(self.layer_code)
        k_conf.get_os_version()
        k_conf.get_config_file()
        k_conf.get_config(self.layer_code, engine_conf)
        self.db_conn = db.QueryDB(k_conf)
        if self.db_conn is None:
            self.errors.append('Ошибка подключения к слою ' + self.layer_code)
            self.log_file('Ошибка подключения к слою ' + self.layer_code, terms=1)
        else:
            self.log_file('Подключение прошло успешно к слою ' + self.layer_code, terms=1)

            if period == '03':
                d_beg = '01.01.'
                d_end = '31.03.'
            elif period == '06':
                d_beg = '01.04.'
                d_end = '30.06.'
            elif period == '09':
                d_beg = '01.07.'
                d_end = '30.09.'
            elif period == '00':
                d_beg = '01.10.'
                d_end = '31.12.'

            self.date = self.my_get_current_datetime(format='date')

            result = None
            if form in ('R1', 'R2'):
                result = self.alco_decl_gen_form(form, org_id, year, period, d_beg, d_end, ignore_error)
            if form in ('R2I', 'R1I'):
                result = self.alco_decl_formI(form, org_id, year, period, d_beg, d_end, ignore_error)
            if form in ('R1II', 'R2II'):
                result = self.alco_decl_formII(form, org_id, year, period, d_beg, d_end, ignore_error)

            if result:
               file_name_to_db = result
               status_db = '1'
            else:
                self.file_name_error = os.path.join(self.path_file, self.layer_code, 'alcodecl', 'error_' + self.date.replace('.', '') + str(uuid.uuid1()).upper() + '.txt')
                str_error = ''
                for itm in self.errors:
                     str_error += itm + c.t_enter
                self.export_file(self.file_name_error, str_error)
                file_name_to_db = self.file_name_error
                status_db = 'E'

            sql_text = '''update MY_ALCDECLARATION_TASK t
                             set t.status = ?,
                                 t.filename = ?
                           where t.queueid = ?'''
            sql_params = [status_db, os.path.basename(file_name_to_db), self.queueid]
            res = self.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch="none",
                                   db_local=self.db_conn)
            if res['status'] == c.kr_sql_error:
                self.log_file('Ошибка подтверждения результата.' + c.kr_term_enter)
                return False

    def my_get_current_datetime(self, format='datetime'):
        date = self.execute_sql("select adate from my_get_now",
                                sql_params = [],
                                fetch="one",
                                db_local=self.db_conn)['datalist']['ADATE']

        if format == 'date':
            return date.split(' ')[0]

    # формирование xml файла алкодекларации
    def alco_decl_gen_form(self, form, org_id, year, period, d_beg, d_end, ignore_error=False):

        d_beg += year
        d_end += year + ' 23:59:59'

        org_info = self.execute_sql("select * from my_spobjects_get(?,?,?)",
                                    sql_params=[None, None, org_id],
                                    fetch="one",
                                    db_local=self.db_conn)
        if org_info['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения ИНН организации.' + c.kr_term_enter)
            self.errors.append('Ошибка получения ИНН организации.')
            return False
        org_info = org_info['datalist']

        if not org_info['INN']:
            self.errors.append('ИНН организации не указан')
            return False

        file_name = '_'.join([form, org_info['INN'], period + year[-1:],
                              self.date.replace('.', ''), str(uuid.uuid1()).upper()])

        def conv(text):
            return str(coalesce(text), encoding='cp1251')

        def get_kpp(objid):
            return self.execute_sql("select c(kpp) as kpp from company where compid = ?",
                                    sql_params=[objid],
                                    fetch="one",
                                    db_local=self.db_conn)['datalist']['KPP']

        def round_v(val):
            return ('%.5f' % val).rstrip('0').rstrip('.')

        def coalesce(val):
            if val:
                return val
            else:
                return ''

        """ Файл """
        file_ = Element(conv('Файл'), {
            conv('ДатаДок'): self.date,
            conv('ВерсФорм'): '4.31',
            conv('НаимПрог'): 'LiteBox'
        })

        """ Файл -> Форма отчетности """
        file_forma_otchetnosti = SubElement(file_, conv('ФормаОтч'), {
            conv('НомФорм'): '11' if form == 'R1' else '12',
            conv('ПризПериодОтч'): period[-1:],
            conv('ГодПериодОтч'): year
        })

        SubElement(file_forma_otchetnosti, conv('Первичная'))

        """ Файл -> Справочники """
        file_spravochniki = SubElement(file_, conv('Справочники'))

        proizv_import_sp = self.execute_sql("select * from my_ad_get_prod_import_turnover(?,?,?,?)",
                                            sql_params=[org_id, form, d_beg, d_end],
                                            fetch="all",
                                            db_local=self.db_conn)
        if proizv_import_sp['status'] == c.kr_sql_error:
            self.log_file('Ошибка выполнения запроса select * from my_ad_get_prod_import_turnover(?,?,?,?).' + c.kr_term_enter)
            self.errors.append('Ошибка выполнения запроса select * from my_ad_get_prod_import_turnover(?,?,?,?).')
            return False

        """ Файл -> Справочники -> Справочник производителей и импортеров продукции """
        listinnkpp = []
        for pr_imp in proizv_import_sp['datalist']:
            if not ignore_error and listinnkpp.count(coalesce(pr_imp['INN']) + coalesce(pr_imp['KPP'])) > 0:
                self.errors.append('Ошибка в справочнике производителей и импортеров ИНН ' +
                                   coalesce(pr_imp['INN']) + ' КПП ' + coalesce(pr_imp['KPP']) + ' повторяются')
            else:
                listinnkpp.append(coalesce(pr_imp['INN']) + coalesce(pr_imp['KPP']))
            if form == 'R1':
                if not ignore_error and (not pr_imp['INN'] or not pr_imp['KPP']):
                    self.errors.append(pr_imp['NAME'] + ' не указаны реквизиты')
                if pr_imp['FIZUR'] == 't':
                    SubElement(file_spravochniki, conv('ПроизводителиИмпортеры'), {
                        conv('ИДПроизвИмп'): str(pr_imp['ID']),
                        conv('П000000000004'): conv(pr_imp['NAME']),
                        conv('П000000000005'): pr_imp['INN']
                    })
                else:
                    SubElement(file_spravochniki, conv('ПроизводителиИмпортеры'), {
                        conv('ИДПроизвИмп'): str(pr_imp['ID']),
                        conv('П000000000004'): conv(pr_imp['NAME']),
                        conv('П000000000005'): pr_imp['INN'],
                        conv('П000000000006'): pr_imp['KPP']
                    })
            else:
                file_spravochniki_proizv_import = SubElement(file_spravochniki, conv('ПроизводителиИмпортеры'), {
                    conv('ИДПроизвИмп'): str(pr_imp['ID']),
                    conv('П000000000004'): conv(pr_imp['NAME'])
                })

                """ Файл -> Справочники -> Справочник поставщиков ->
                    Поставщик (импортер) Юр. Лицо|Поставщик Физ. Лицо """
                if pr_imp['FIZUR'] == 'u':
                    if not ignore_error and (not pr_imp['INN'] or not pr_imp['KPP']):
                        self.errors.append(pr_imp['NAME'] + ' не указаны реквизиты')
                    SubElement(file_spravochniki_proizv_import, conv(pr_imp['FIZURNAME']), {
                        conv('П000000000005'): coalesce(pr_imp['INN']),
                        conv('П000000000006'): coalesce(pr_imp['KPP'])
                    })
                elif pr_imp['FIZUR'] in ['f','t']:
                    if not ignore_error and (not pr_imp['INN']):
                        self.errors.append(pr_imp['NAME'] + ' не указаны реквизиты')
                    SubElement(file_spravochniki_proizv_import, conv(pr_imp['FIZURNAME']), {
                        conv('П000000000005'): pr_imp['INN']
                    })

        postavschici_sp = self.execute_sql("select * from my_ad_get_suppl_turnover(?,?,?,?)",
                                           sql_params=[org_id, form, d_beg, d_end],
                                           fetch="all",
                                           db_local=self.db_conn)

        if postavschici_sp['status'] == c.kr_sql_error:
            self.log_file('select * from my_ad_get_suppl_turnover(?,?,?,?)' + c.kr_term_enter)
            self.errors.append('select * from my_ad_get_suppl_turnover(?,?,?,?)')
            return False

        """ Файл -> Справочники -> Справочник поставщиков """
        listinnkpp = []
        for post in postavschici_sp['datalist']:
            file_spravochniki_postavschiki = SubElement(file_spravochniki, conv('Поставщики'), {
                conv('ИдПостав'): str(post['SUPPLID']),
                conv('П000000000007'): conv(post['NAME'])
            })

            if form == 'R1':
                postavschici_sp_licenzii = self.execute_sql("select * from my_ad_get_suppl_licenses(?,?,?)",
                                                            sql_params=[post['SUPPLID'], d_beg, d_end.split(' ')[0]],
                                                            fetch="all",
                                                            db_local=self.db_conn)
                if postavschici_sp_licenzii['status'] == c.kr_sql_error:
                    self.log_file('select * from my_ad_get_suppl_licenses(?,?,?)' + c.kr_term_enter)
                    self.errors.append('select * from my_ad_get_suppl_licenses(?,?,?)')
                    return False

                if not ignore_error and (not postavschici_sp_licenzii['datalist']):
                    self.errors.append(post['NAME'] + ' не указана лицензия')
                """ Файл -> Справочники -> Справочник поставщиков -> Лицензии контрагента """

                """ Файл -> Справочники -> Справочник поставщиков -> Лицензии контрагента -> Лицензия контрагента """
                for post_lic in postavschici_sp_licenzii['datalist']:
                    file_spravochniki_postavschiki_licenzii = SubElement(file_spravochniki_postavschiki,
                                                                         conv('Лицензии'))
                    SubElement(file_spravochniki_postavschiki_licenzii, conv('Лицензия'), {
                        conv('ИдЛицензии'): str(post_lic['LICID']),
                        conv('П000000000011'): conv(post_lic['REGNUMBER']),
                        conv('П000000000012'): formatMxDateTime(post_lic['STARTDATE'], '%d.%m.%Y'),
                        conv('П000000000013'): formatMxDateTime(post_lic['ENDDATE'], '%d.%m.%Y'),
                        conv('П000000000014'): conv(post_lic['ISSUEBY'])
                    })

            """ Файл -> Справочники -> Справочник поставщиков -> ПоставщикЮр. Лицо|Поставщик Физ. Лицо """
            if not ignore_error and listinnkpp.count(coalesce(post['INN']) + coalesce(post['KPP'])) > 0:
                self.errors.append('Ошибка в справочнике поставщиков ИНН ' + coalesce(post['INN']) + ' КПП '
                                   + coalesce(post['KPP']) + ' повторяются')
            else:
                listinnkpp.append(coalesce(post['INN']) + coalesce(post['KPP']))
            if post['FIZUR'] == 'u':
                if not ignore_error and (not post['INN'] or not post['KPP']):
                    self.errors.append(post['NAME'] + ' не указаны реквизиты')
                SubElement(file_spravochniki_postavschiki, conv(post['FIZURNAME']), {
                    conv('П000000000009'): coalesce(post['INN']),
                    conv('П000000000010'): coalesce(post['KPP'])
                })
            elif post['FIZUR'] in ['f','t']:
                if not ignore_error and (not post['INN']):
                    self.errors.append(post['NAME'] + ' не указаны реквизиты')
                SubElement(file_spravochniki_postavschiki, conv(post['FIZURNAME']), {
                    conv('П000000000009'): coalesce(post['INN'])
                })

        """ Файл -> Общие сведения информационной части """
        file_document = SubElement(file_, conv('Документ'))

        """ Файл -> Общие сведения информационной части -> Сведения об организации """
        file_document_organizaciya = SubElement(file_document, conv('Организация'))

        """ Файл -> Общие сведения информационной части -> Сведения об организации -> Реквизиты организации """
        if form == 'R1':
            file_document_organizaciya_rekvizity = SubElement(file_document_organizaciya, conv('Реквизиты'), {
                conv('Наим'): conv(org_info['FULLNAME']),
                conv('ТелОрг'): str(org_info['PHONE'] or ''),
                conv('EmailОтпр'): str(org_info['EMAIL'] or '')
            })
        else:
            file_document_organizaciya_rekvizity = SubElement(file_document_organizaciya, conv('Реквизиты'), {
                conv('Наим'): conv(org_info['FULLNAME']),
                conv('ТелОрг'): str(org_info['PHONE'] or ''),
                conv('EmailОтпр'): str(org_info['EMAIL'] or '')
            })

        org_address = self.execute_sql('''select a.*, cu.oksm
                                                from company co
                                                     left join address a on a.addid = co.address
                                                     left join country cu on cu.countryid = a.countryid
                                          where co.compid = ?''',
                                       sql_params=[org_id],
                                       fetch="one",
                                       db_local=self.db_conn)
        if org_address['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения адреса организации.' + c.kr_term_enter)
            self.errors.append('Ошибка получения адреса организации.')
            return False
        org_address = org_address['datalist']

        """ Файл -> Общие сведения информационной части -> Сведения об организации -> Реквизиты организации ->
            Местонахождение организации (обособленного подразделения) """
        file_document_organizaciya_rekvizity_adres = SubElement(file_document_organizaciya_rekvizity,
                                                                conv('АдрОрг'))
        if not ignore_error and (not org_address['OKSM']):
            self.errors.append(str(org_info['FULLNAME']) + ' не указана страна')
        file_document_organizaciya_rekvizity_adres_strana = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                       conv('КодСтраны'))
        file_document_organizaciya_rekvizity_adres_strana.text = str(org_address['OKSM'] or '')

        file_document_organizaciya_rekvizity_adres_index = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                      conv('Индекс'))
        file_document_organizaciya_rekvizity_adres_index.text = str(org_address['POSTINDEX'] or '')
        if not ignore_error and (not org_address['STATECODE']):
            self.errors.append(str(org_info['FULLNAME']) + ' не указан регион')
        file_document_organizaciya_rekvizity_adres_region = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                       conv('КодРегион'))
        file_document_organizaciya_rekvizity_adres_region.text = str(org_address['STATECODE'] or '')

        file_document_organizaciya_rekvizity_adres_rayon = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                      conv('Район'))
        file_document_organizaciya_rekvizity_adres_rayon.text = conv(org_address['RAYON'] or '')

        file_document_organizaciya_rekvizity_adres_gorod = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                      conv('Город'))
        file_document_organizaciya_rekvizity_adres_gorod.text = conv(org_address['CITY'] or '')

        file_document_organizaciya_rekvizity_adres_nasel_punkt = SubElement(
            file_document_organizaciya_rekvizity_adres,
            conv('НаселПункт'))
        file_document_organizaciya_rekvizity_adres_nasel_punkt.text = conv(org_address['TOWN'] or '')

        file_document_organizaciya_rekvizity_adres_ylica = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                      conv('Улица'))
        file_document_organizaciya_rekvizity_adres_ylica.text = conv(org_address['STREET'] or '')

        file_document_organizaciya_rekvizity_adres_dom = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                    conv('Дом'))
        file_document_organizaciya_rekvizity_adres_dom.text = str(org_address['HOUSE'] or '')

        file_document_organizaciya_rekvizity_adres_korpys = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                       conv('Корпус'))
        file_document_organizaciya_rekvizity_adres_korpys.text = conv(org_address['BUILDING'] or '')

        file_document_organizaciya_rekvizity_adres_litera = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                       conv('Литера'))
        file_document_organizaciya_rekvizity_adres_litera.text = conv(org_address['LITERA'] or '')

        file_document_organizaciya_rekvizity_adres_kvartira = SubElement(file_document_organizaciya_rekvizity_adres,
                                                                         conv('Кварт'))
        file_document_organizaciya_rekvizity_adres_kvartira.text = conv(org_address['ROOM'] or '')
        if form == 'R1':
            if not ignore_error and (not org_info['INN'] or not org_info['KPP']):
                self.errors.append(str(org_info['FULLNAME']) + ' не указаны реквизиты')
            SubElement(file_document_organizaciya_rekvizity, conv('ЮЛ'), {
                conv('ИННЮЛ'): str(coalesce(org_info['INN'])),
                conv('КППЮЛ'): str(coalesce(org_info['KPP']))
            })
        else:
            if org_info['FIZUR'] == 'u':
                if not ignore_error and (not org_info['INN'] or not org_info['KPP']):
                    self.errors.append(org_info['FULLNAME'] + ' не указаны реквизиты')
                SubElement(file_document_organizaciya_rekvizity, conv('ЮЛ'), {
                    conv('ИННЮЛ'): str(coalesce(org_info['INN'])),
                    conv('КППЮЛ'): str(coalesce(org_info['KPP']))
                })
            elif org_info['FIZUR'] == 'f':
                if not ignore_error and (not org_info['INN']):
                    self.errors.append(org_info['FULLNAME'] + ' не указаны реквизиты')
                SubElement(file_document_organizaciya_rekvizity, conv('ФЛ'), {
                    conv('ИННФЛ'): str(coalesce(org_info['INN']))
                })
        """ Файл -> Общие сведения информационной части -> Сведения об организации -> Сведения об ответственных лицах """
        file_document_organizaciya_otvetstennoe_lico = SubElement(file_document_organizaciya, conv('ОтветЛицо'))

        chief = (org_info['CHIEF'] or '').strip().split(' ')

        """ Файл -> Общие сведения информационной части -> Сведения об организации ->
            Сведения об ответственных лицах -> Сведения о руководителе """
        file_document_organizaciya_otvetstennoe_lico_rykov = SubElement(
            file_document_organizaciya_otvetstennoe_lico,
            conv('Руководитель'))

        file_document_organizaciya_otvetstennoe_lico_rykov_familiya = \
            SubElement(file_document_organizaciya_otvetstennoe_lico_rykov, conv('Фамилия'))
        file_document_organizaciya_otvetstennoe_lico_rykov_familiya.text = conv(chief[0] if len(chief) else '')

        file_document_organizaciya_otvetstennoe_lico_rykov_imya = \
            SubElement(file_document_organizaciya_otvetstennoe_lico_rykov, conv('Имя'))
        file_document_organizaciya_otvetstennoe_lico_rykov_imya.text = conv(chief[1] if len(chief) > 1 else '')

        file_document_organizaciya_otvetstennoe_lico_rykov_otchestvo = \
            SubElement(file_document_organizaciya_otvetstennoe_lico_rykov, conv('Отчество'))
        file_document_organizaciya_otvetstennoe_lico_rykov_otchestvo.text = conv(chief[2] if len(chief) > 2 else '')

        main_acc = (org_info['MAINACC'] or '').split(' ')

        """ Файл -> Общие сведения информационной части -> Сведения об организации ->
            Сведения об ответственных лицах -> Сведения о главном бухгалтере """
        file_document_organizaciya_otvetstennoe_lico_glav_byh = \
            SubElement(file_document_organizaciya_otvetstennoe_lico, conv('Главбух'))

        file_document_organizaciya_otvetstennoe_lico_glav_byh_familiya = \
            SubElement(file_document_organizaciya_otvetstennoe_lico_glav_byh, conv('Фамилия'))
        file_document_organizaciya_otvetstennoe_lico_glav_byh_familiya.text = conv(
            main_acc[0] if len(main_acc) else '')

        file_document_organizaciya_otvetstennoe_lico_glav_byh_imya = \
            SubElement(file_document_organizaciya_otvetstennoe_lico_glav_byh, conv('Имя'))
        file_document_organizaciya_otvetstennoe_lico_glav_byh_imya.text = conv(
            main_acc[1] if len(main_acc) > 1 else '')

        file_document_organizaciya_otvetstennoe_lico_glav_byh_otchestvo = \
            SubElement(file_document_organizaciya_otvetstennoe_lico_glav_byh, conv('Отчество'))
        file_document_organizaciya_otvetstennoe_lico_glav_byh_otchestvo.text = \
            conv(main_acc[2] if len(main_acc) > 2 else '')

        if form == 'R1':
            licenses = self.execute_sql("select * from my_licenses_get(?,?) where lictypecode = ?",
                                        sql_params=[org_id, None, 'RETAIL'],
                                        fetch="all",
                                        db_local=self.db_conn)
            if licenses['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения лицензий организации.' + c.kr_term_enter)
                self.errors.append('Ошибка получения лицензий организации.')
                return False
            """ Файл -> Общие сведения информационной части -> Сведения об организации -> Сведения о деятельности """
            file_document_organizaciya_deyatelnost = SubElement(file_document_organizaciya, conv('Деятельность'))

            """ Файл -> Общие сведения информационной части -> Сведения об организации -> Сведения о деятельности ->
                Список лицензий организации | Нелицензируемая деятельность """
            file_document_organizaciya_deyatelnost_licenziryemaya = SubElement(
                file_document_organizaciya_deyatelnost,
                conv('Лицензируемая'))

            """ Файл -> Общие сведения информационной части -> Сведения об организации -> Сведения о деятельности ->
                Список лицензий организации | Нелицензируемая деятельность -> Сведения о лицензиях """
            if not ignore_error and (not licenses['datalist']):
                self.errors.append(str(org_info['FULLNAME']) + ' не указана лицензия')
            for lic in licenses['datalist']:
                SubElement(file_document_organizaciya_deyatelnost_licenziryemaya, conv('Лицензия'), {
                    conv('ВидДеят'): '06',
                    conv('СерНомЛиц'): conv(lic['SERIES']) + ',' + str(lic['NUMBER']),
                    conv('ДатаНачЛиц'): formatMxDateTime(lic['ISSUEDATE'], '%d.%m.%Y'),
                    conv('ДатаОконЛиц'): formatMxDateTime(lic['TERMDATE'] or lic['ENDDATE'], '%d.%m.%Y')
                })

        shops = self.execute_sql("select * from rbs_list_objects(?,?,?,?,?,?)",
                                 sql_params=[org_id, None, 'SHOPS', None, None, None],
                                 fetch="all",
                                 db_local=self.db_conn)
        if shops['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения магазинов организации.' + c.kr_term_enter)
            self.errors.append('Ошибка получения магазинов организации.')
            return False

        for shop in shops['datalist']:
            oborot = self.execute_sql("select * from my_ad_get_turnover(?,?,?,?)",
                                      sql_params=[shop['OBJID'], form, d_beg, d_end],
                                      fetch="all",
                                      db_local=self.db_conn)
            if oborot['status'] == c.kr_sql_error:
                self.log_file('Ошибка select * from my_ad_get_turnover(?,?,?,?).' + c.kr_term_enter)
                self.errors.append('Ошибка select * from my_ad_get_turnover(?,?,?,?).')
                return False

            est_oborot = 'false'

            if len(oborot['datalist']):
                est_oborot = 'true'

            """ Файл -> Общие сведения информационной части -> Сведения об объеме розничной продажи алкогольной и
                спиртосодержащей продукции / Сведения об объеме розничной продажи пива и пивных напитков """
            if not ignore_error and org_info['FIZUR'] == 'u' and (not get_kpp(shop['OBJID'])):
                self.errors.append(shop['NAME'] + ' не указан КПП')
            tagdata = {
                conv('Наим'): conv(shop['NAME']),
                conv('НаличиеОборота'): est_oborot
            }
            if org_info['FIZUR'] == 'u':
                tagdata.update({conv('КППЮЛ'): get_kpp(shop['OBJID'])})
            file_document_obyem_oborot = SubElement(file_document, conv('ОбъемОборота'), tagdata)

            shop_address = self.execute_sql('''select a.*, cu.oksm
                                                     from company co
                                                          left join address a on a.addid = co.address
                                                          left join country cu on cu.countryid = a.countryid
                                                    where co.compid = ?''',
                                            sql_params=[shop['OBJID']],
                                            fetch="one",
                                            db_local=self.db_conn)
            if shop_address['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения адреса магазина.' + c.kr_term_enter)
                self.errors.append('Ошибка получения адреса магазина.')
                return False
            shop_address = shop_address['datalist']

            """ Файл -> Общие сведения информационной части -> Сведения об объеме розничной продажи алкогольной и
                спиртосодержащей продукции -> Адрес места осуществления деятельности """
            file_document_oborot_adres = SubElement(file_document_obyem_oborot, conv('АдрОрг'))
            if not ignore_error and (not shop_address['OKSM']):
                self.errors.append(shop['NAME'] + ' не указана страна')
            file_document_oborot_adres_strana = SubElement(file_document_oborot_adres, conv('КодСтраны'))
            file_document_oborot_adres_strana.text = str(shop_address['OKSM'] or '')

            file_document_oborot_adres_index = SubElement(file_document_oborot_adres, conv('Индекс'))
            file_document_oborot_adres_index.text = str(shop_address['POSTINDEX'] or '')
            if not ignore_error and (not shop_address['STATECODE']):
                self.errors.append(shop['NAME'] + ' не указан регион')
            file_document_oborot_adres_region = SubElement(file_document_oborot_adres, conv('КодРегион'))
            file_document_oborot_adres_region.text = str(shop_address['STATECODE'] or '')

            file_document_oborot_adres_rayon = SubElement(file_document_oborot_adres, conv('Район'))
            file_document_oborot_adres_rayon.text = conv(shop_address['RAYON'] or '')

            file_document_oborot_adres_gorod = SubElement(file_document_oborot_adres, conv('Город'))
            file_document_oborot_adres_gorod.text = conv(shop_address['CITY'] or '')

            file_document_oborot_adres_nasel_punkt = SubElement(file_document_oborot_adres, conv('НаселПункт'))
            file_document_oborot_adres_nasel_punkt.text = conv(shop_address['TOWN'] or '')

            file_document_oborot_adres_ylica = SubElement(file_document_oborot_adres, conv('Улица'))
            file_document_oborot_adres_ylica.text = conv(shop_address['STREET'] or '')

            file_document_oborot_adres_dom = SubElement(file_document_oborot_adres, conv('Дом'))
            file_document_oborot_adres_dom.text = str(shop_address['HOUSE'] or '')

            file_document_oborot_adres_korpys = SubElement(file_document_oborot_adres, conv('Корпус'))
            file_document_oborot_adres_korpys.text = conv(shop_address['BUILDING'] or '')

            file_document_oborot_adres_litera = SubElement(file_document_oborot_adres, conv('Литера'))
            file_document_oborot_adres_litera.text = conv(shop_address['LITERA'] or '')

            file_document_oborot_adres_kvartira = SubElement(file_document_oborot_adres, conv('Кварт'))
            file_document_oborot_adres_kvartira.text = conv(shop_address['ROOM'] or '')

            if est_oborot == 'true':
                oborot_nomer = 1

                for ob in oborot['datalist']:
                    proizvoditel_importer = self.execute_sql("select * from my_ad_get_prod_importer(?,?,?,?)",
                                                             sql_params=[shop['OBJID'], d_beg, d_end, ob['WARESKINDID']],
                                                             fetch="all",
                                                             db_local=self.db_conn)
                    if proizvoditel_importer['status'] == c.kr_sql_error:
                        self.log_file('Ошибка получения производителя/импортера.' + c.kr_term_enter)
                        self.errors.append('Ошибка получения производителя/импортера.')
                        return False

                    """ Файл -> Общие сведения информационной части -> Сведения об объеме розничной продажи алкогольной
                        и спиртосодержащей продукции / Сведения об объеме розничной продажи пива и пивных напитков ->
                        Розничный оборот алкогольной и спиртосодержащей продукции / Розничный оборот пива и пивных
                        напитков """
                    file_document_obyem_oborot_oborot = SubElement(file_document_obyem_oborot, conv('Оборот'), {
                        conv('ПN'): str(oborot_nomer),
                        conv('П000000000003'): ob['CODE']
                    })

                    oborot_nomer += 1

                    proizvoditel_importer_nomer = 1

                    for pi in proizvoditel_importer['datalist']:
                        """ Файл -> Общие сведения информационной части -> Сведения об объеме розничной продажи
                            алкогольной и спиртосодержащей продукции / Сведения об объеме розничной продажи пива и
                            пивных напитков -> Розничный оборот алкогольной и спиртосодержащей продукции / Розничный
                            оборот пива и пивных напитков -> Сведения о производителе либо импортере продукции """
                        file_document_obyem_oborot_oborot_proizvoditel_importer = \
                            SubElement(file_document_obyem_oborot_oborot, conv('СведПроизвИмпорт'), {
                                conv('ПN'): str(proizvoditel_importer_nomer),
                                conv('ИдПроизвИмп'): str(pi['ID']),
                            })

                        postavschic = self.execute_sql("select * from my_ad_get_suppl(?,?,?,?,?)",
                                                       sql_params=[shop['OBJID'], ob['WARESKINDID'], pi['ID'],
                                                                   d_beg, d_end.split(' ')[0]],
                                                       fetch="all",
                                                       db_local=self.db_conn)
                        if proizvoditel_importer['status'] == c.kr_sql_error:
                            self.log_file('Ошибка получения поставщика.' + c.kr_term_enter)
                            self.errors.append('Ошибка получения поставщика.')
                            return False

                        postavschic_nomer = 1
                        dvizhenie_nomer = 1

                        for post in postavschic['datalist']:
                            """ Файл -> Общие сведения информационной части -> Сведения об объеме розничной продажи
                                алкогольной и спиртосодержащей продукции / Сведения об объеме розничной продажи пива и
                                пивных напитков -> Розничный оборот алкогольной и спиртосодержащей продукции / Розничный
                                оборот пива и пивных напитков -> Сведения о производителе либо импортере продукции ->
                                Сведения о поставщике """
                            if form == 'R1':
                                file_document_obyem_oborot_oborot_proizvoditel_importer_postavschic = \
                                    SubElement(file_document_obyem_oborot_oborot_proizvoditel_importer,
                                               conv('Поставщик'), {
                                                   conv('ПN'): str(postavschic_nomer),
                                                   conv('ИдПоставщика'): str(post['SUPPLID']),
                                                   conv('ИдЛицензии'): str(post['LICID'] or '')
                                               })
                            else:
                                file_document_obyem_oborot_oborot_proizvoditel_importer_postavschic = \
                                    SubElement(file_document_obyem_oborot_oborot_proizvoditel_importer,
                                               conv('Поставщик'), {
                                                   conv('ПN'): str(postavschic_nomer),
                                                   conv('ИдПоставщика'): str(post['SUPPLID'])
                                               })

                            postavschic_product = self.execute_sql('''select *
                                                                            from my_ad_get_suppl_product(?,?,?,?,?,?,?)''',
                                                                   sql_params=[shop['OBJID'], ob['WARESKINDID'], pi['ID'],
                                                                               post['SUPPLID'], post['SUPPLID'],
                                                                               d_beg, d_end],
                                                                   fetch="all",
                                                                   db_local=self.db_conn)
                            if postavschic_product['status'] == c.kr_sql_error:
                                self.log_file('Ошибка получения поставщика продуцкции.' + c.kr_term_enter)
                                self.errors.append('Ошибка получения поставщика продуцкции.')
                                return False

                            for post_prod in postavschic_product['datalist']:
                                """ Файл -> Общие сведения информационной части -> Сведения об объеме розничной продажи
                                    алкогольной и спиртосодержащей продукции / Сведения об объеме розничной продажи пива
                                    и пивных напитков -> Розничный оборот алкогольной и спиртосодержащей продукции /
                                    Розничный оборот пива и пивных напитков -> Сведения о производителе либо импортере
                                    продукции -> Сведения о поставщике -> Сведения о продукции """
                                if not ignore_error and (not post_prod['PVOLUME']):
                                    self.errors.append('нулевая поставка в ' + shop['NAME'] + ' ' + coalesce(
                                        post_prod['TTN']) + ' от ' +
                                                    formatMxDateTime(post_prod['DOCDATE'],
                                                                     '%d.%m.%Y') + ' по коду ' + ob['CODE'] +
                                                    ' поставщик - ' + post['FULLNAME'] + ', производитель ' + pi[
                                                        'FULLNAME'] +
                                                    '. Возможно не указан объем поставляемой продукции.')
                                SubElement(file_document_obyem_oborot_oborot_proizvoditel_importer_postavschic,
                                           conv('Продукция'), {
                                               conv('П200000000013'): formatMxDateTime(post_prod['DOCDATE'],
                                                                                       '%d.%m.%Y'),
                                               conv('П200000000014'): conv(coalesce(post_prod['TTN'])),
                                               conv('П200000000015'): conv(coalesce(post_prod['GTD'])),
                                               conv('П200000000016'): round_v(post_prod['PVOLUME'])
                                           })

                            postavschic_nomer += 1

                        proizvoditel_importer_nomer += 1

                        dvizhenie = self.execute_sql("select * from my_ad_get_movement(?,?,?,?,?)",
                                                     sql_params=[shop['OBJID'], ob['WARESKINDID'], pi['ID'],
                                                                 d_beg, d_end],
                                                     fetch="one",
                                                     db_local=self.db_conn)
                        if dvizhenie['status'] == c.kr_sql_error:
                            self.log_file('Ошибка получения движения продуцкции.' + c.kr_term_enter)
                            self.errors.append('Ошибка получения движения продуцкции.')
                            return False
                        dvizhenie = dvizhenie['datalist']

                        """ Файл -> Общие сведения информационной части -> Сведения об объеме розничной продажи
                            алкогольной и спиртосодержащей продукции / Сведения об объеме розничной продажи пива
                            и пивных напитков -> Розничный оборот алкогольной и спиртосодержащей продукции /
                            Розничный оборот пива и пивных напитков -> Сведения о производителе либо импортере
                            продукции -> Движение продукции """
                        if form == 'R1':
                            SubElement(file_document_obyem_oborot_oborot_proizvoditel_importer, conv('Движение'), {
                                conv('ПN'): str(dvizhenie_nomer),
                                conv('П100000000006'): round_v(dvizhenie['REST_BEG']),
                                conv('П100000000007'): round_v(dvizhenie['PRODUCTION_INC']),
                                conv('П100000000008'): round_v(dvizhenie['WHOLESALE_INC']),
                                conv('П100000000009'): round_v(dvizhenie['IMPORT_INC']),
                                conv('П100000000010'): round_v(dvizhenie['TOTAL_INC']),
                                conv('П100000000011'): round_v(dvizhenie['RET_INC']),
                                conv('П100000000012'): round_v(dvizhenie['OTHER_INC']),
                                conv('П100000000013'): round_v(dvizhenie['MOVING_INC']),
                                conv('П100000000014'): round_v(dvizhenie['TOTAL_INC_ALL']),
                                conv('П100000000015'): round_v(dvizhenie['SALE_OUT']),
                                conv('П100000000016'): round_v(dvizhenie['OTHER_OUT']),
                                conv('П100000000017'): round_v(dvizhenie['SUPLRET_OUT']),
                                conv('П100000000018'): round_v(dvizhenie['MOVING_OUT']),
                                conv('П100000000019'): round_v(dvizhenie['TOTAL_OUT']),
                                conv('П100000000020'): round_v(dvizhenie['REST_END']),
                                conv('П100000000021'): '0'
                            })
                        else:
                            SubElement(file_document_obyem_oborot_oborot_proizvoditel_importer, conv('Движение'), {
                                conv('ПN'): str(dvizhenie_nomer),
                                conv('П100000000006'): round_v(dvizhenie['REST_BEG']),
                                conv('П100000000007'): round_v(dvizhenie['PRODUCTION_INC']),
                                conv('П100000000008'): round_v(dvizhenie['WHOLESALE_INC']),
                                conv('П100000000009'): round_v(dvizhenie['IMPORT_INC']),
                                conv('П100000000010'): round_v(dvizhenie['TOTAL_INC']),
                                conv('П100000000011'): round_v(dvizhenie['RET_INC']),
                                conv('П100000000012'): round_v(dvizhenie['OTHER_INC'] + dvizhenie['MOVING_INC']),
                                conv('П100000000013'): round_v(dvizhenie['TOTAL_INC_ALL']),
                                conv('П100000000014'): round_v(dvizhenie['SALE_OUT']),
                                conv('П100000000015'): round_v(dvizhenie['OTHER_OUT'] + dvizhenie['MOVING_OUT']),
                                conv('П100000000016'): round_v(dvizhenie['SUPLRET_OUT']),
                                conv('П100000000017'): round_v(dvizhenie['TOTAL_OUT']),
                                conv('П100000000018'): round_v(dvizhenie['REST_END'])
                            })

                        dvizhenie_nomer += 1

        if len(self.errors) != 0:
            return False
        output = minidom.parseString(tostring(file_, encoding='windows-1251')). \
            toprettyxml(indent='    ', encoding='windows-1251')
        file_name = os.path.join(self.path_file, self.layer_code, 'alcodecl', file_name + '.xml')
        self.export_file(file_name, output)
        return file_name

    def coalesce(self, val):
        if val:
            return val
        else:
            return ''

    # формирование excel файла алкодекларации разделI
    def alco_decl_formI(self, form, org_id, year, period, d_beg, d_end, ignoreerror=None):
        if form == 'R1I':
            form = 'R1'
            tmpl = 'formI_11.lb'
        elif form == 'R2I':
            form = 'R2'
            tmpl = 'formI_12.lb'
        else:
            self.errors('Не указана форма')
            return False
        d_beg += year
        d_end += year + ' 23:59:59'

        org_info = self.execute_sql("select FULLNAME, INN, KPP, ADDRESS from GETOBJECTFIELDS(?)",
                                    sql_params=[org_id],
                                    fetch="one",
                                    db_local=self.db_conn)

        if org_info['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения ИНН организации.' + c.kr_term_enter)
            self.errors.append('Ошибка получения ИНН организации.')
            return False
        org_info = org_info['datalist']

        if not org_info['INN']:
            self.errors.append('ИНН организации не указан')
            return False

        file_name = '_'.join([form, org_info['INN'], period + year[-1:],
                              self.date.replace('.', ''), str(uuid.uuid1()).upper()])
        org_inn = org_info['INN']
        dic = {}
        dic['org_info'] = org_info
        org_wareskind_oborot = self.execute_sql("select * from MY_AD_GET_TURNOVER_ORG(?,?,?,?)",
                                                sql_params=[org_id, form, d_beg, d_end],
                                                fetch="all",
                                                db_local=self.db_conn)
        if org_wareskind_oborot['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения select * from MY_AD_GET_TURNOVER_ORG(?,?,?,?).' + c.kr_term_enter)
            self.errors.append('Ошибка получения select * from MY_AD_GET_TURNOVER_ORG(?,?,?,?).')
            return False
        org_wareskind_oborot = org_wareskind_oborot['datalist']

        arr = []
        dvizhenietotall = {}
        dvizhenietotall['STR'] = 'ИТОГО:'
        dvizhenietotall['REST_BEG'] = 0
        dvizhenietotall['PRODUCTION_INC'] = 0
        dvizhenietotall['WHOLESALE_INC'] = 0
        dvizhenietotall['IMPORT_INC'] = 0
        dvizhenietotall['TOTAL_INC'] = 0
        dvizhenietotall['RET_INC'] = 0
        dvizhenietotall['OTHER_INC'] = 0
        dvizhenietotall['MOVING_INC'] = 0
        dvizhenietotall['TOTAL_INC_ALL'] = 0
        dvizhenietotall['SALE_OUT'] = 0
        dvizhenietotall['OTHER_OUT'] = 0
        dvizhenietotall['SUPLRET_OUT'] = 0
        dvizhenietotall['MOVING_OUT'] = 0
        dvizhenietotall['TOTAL_OUT'] = 0
        dvizhenietotall['REST_END'] = 0
        for wareskind in org_wareskind_oborot:
            wareskindarr = {}
            proizvoditel_importer = self.execute_sql("select * from my_ad_get_prod_importer_org(?,?,?,?)",
                                                     sql_params=[org_id, d_beg, d_end, wareskind['WARESKINDID']],
                                                     fetch="all",
                                                     db_local=self.db_conn)
            if proizvoditel_importer['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения производителя/импортера.' + c.kr_term_enter)
                self.errors.append('Ошибка получения производителя/импортера.')
                return False
            proizvoditel_importer = proizvoditel_importer['datalist']
            wareskindarr['CODE'] = wareskind['CODE']
            wareskindarr['NAME'] = wareskind['NAME']

            dvizhenietotallcode = {}
            dvizhenietotallcode['STR'] = 'Итого по коду: ' + wareskindarr['CODE']
            dvizhenietotallcode['REST_BEG'] = 0
            dvizhenietotallcode['PRODUCTION_INC'] = 0
            dvizhenietotallcode['WHOLESALE_INC'] = 0
            dvizhenietotallcode['IMPORT_INC'] = 0
            dvizhenietotallcode['TOTAL_INC'] = 0
            dvizhenietotallcode['RET_INC'] = 0
            dvizhenietotallcode['OTHER_INC'] = 0
            dvizhenietotallcode['MOVING_INC'] = 0
            dvizhenietotallcode['TOTAL_INC_ALL'] = 0
            dvizhenietotallcode['SALE_OUT'] = 0
            dvizhenietotallcode['OTHER_OUT'] = 0
            dvizhenietotallcode['SUPLRET_OUT'] = 0
            dvizhenietotallcode['MOVING_OUT'] = 0
            dvizhenietotallcode['TOTAL_OUT'] = 0
            dvizhenietotallcode['REST_END'] = 0

            for pi in proizvoditel_importer:
                arrpi = {}
                dvizhenie = self.execute_sql("select * from my_ad_get_movement_org(?,?,?,?,?)",
                                             sql_params=[org_id, wareskind['WARESKINDID'], pi['ID'], d_beg, d_end],
                                             fetch="one",
                                             db_local=self.db_conn)
                if dvizhenie['status'] == c.kr_sql_error:
                    self.log_file('Ошибка получения движения.' + c.kr_term_enter)
                    self.errors.append('Ошибка получения движения.')
                    return False
                dvizhenie = dvizhenie['datalist']
                arrpi['IMPNAME'] = pi['FULLNAME']
                arrpi['IMPINN'] = pi['INN']
                arrpi['IMPKPP'] = pi['KPP']
                arrpi['STR'] = ''
                arrpi['wareskind'] = wareskindarr
                arrpi['movement'] = dvizhenie

                dvizhenietotallcode['REST_BEG'] = dvizhenietotallcode['REST_BEG'] + dvizhenie['REST_BEG']
                dvizhenietotallcode['PRODUCTION_INC'] = dvizhenietotallcode['PRODUCTION_INC'] + dvizhenie[
                    'PRODUCTION_INC']
                dvizhenietotallcode['WHOLESALE_INC'] = dvizhenietotallcode['WHOLESALE_INC'] + dvizhenie[
                    'WHOLESALE_INC']
                dvizhenietotallcode['IMPORT_INC'] = dvizhenietotallcode['IMPORT_INC'] + dvizhenie['IMPORT_INC']
                dvizhenietotallcode['TOTAL_INC'] = dvizhenietotallcode['TOTAL_INC'] + dvizhenie['TOTAL_INC']
                dvizhenietotallcode['RET_INC'] = dvizhenietotallcode['RET_INC'] + dvizhenie['RET_INC']
                dvizhenietotallcode['OTHER_INC'] = dvizhenietotallcode['OTHER_INC'] + dvizhenie['OTHER_INC']
                dvizhenietotallcode['MOVING_INC'] = dvizhenietotallcode['MOVING_INC'] + dvizhenie['MOVING_INC']
                dvizhenietotallcode['TOTAL_INC_ALL'] = dvizhenietotallcode['TOTAL_INC_ALL'] + dvizhenie[
                    'TOTAL_INC_ALL']
                dvizhenietotallcode['SALE_OUT'] = dvizhenietotallcode['SALE_OUT'] + dvizhenie['SALE_OUT']
                dvizhenietotallcode['OTHER_OUT'] = dvizhenietotallcode['OTHER_OUT'] + dvizhenie['OTHER_OUT']
                dvizhenietotallcode['SUPLRET_OUT'] = dvizhenietotallcode['SUPLRET_OUT'] + dvizhenie['SUPLRET_OUT']
                dvizhenietotallcode['MOVING_OUT'] = dvizhenietotallcode['MOVING_OUT'] + dvizhenie['MOVING_OUT']
                dvizhenietotallcode['TOTAL_OUT'] = dvizhenietotallcode['TOTAL_OUT'] + dvizhenie['TOTAL_OUT']
                dvizhenietotallcode['REST_END'] = dvizhenietotallcode['REST_END'] + dvizhenie['REST_END']

                dvizhenietotall['REST_BEG'] = dvizhenietotall['REST_BEG'] + dvizhenie['REST_BEG']
                dvizhenietotall['PRODUCTION_INC'] = dvizhenietotall['PRODUCTION_INC'] + dvizhenie['PRODUCTION_INC']
                dvizhenietotall['WHOLESALE_INC'] = dvizhenietotall['WHOLESALE_INC'] + dvizhenie['WHOLESALE_INC']
                dvizhenietotall['IMPORT_INC'] = dvizhenietotall['IMPORT_INC'] + dvizhenie['IMPORT_INC']
                dvizhenietotall['TOTAL_INC'] = dvizhenietotall['TOTAL_INC'] + dvizhenie['TOTAL_INC']
                dvizhenietotall['RET_INC'] = dvizhenietotall['RET_INC'] + dvizhenie['RET_INC']
                dvizhenietotall['OTHER_INC'] = dvizhenietotall['OTHER_INC'] + dvizhenie['OTHER_INC']
                dvizhenietotall['MOVING_INC'] = dvizhenietotall['MOVING_INC'] + dvizhenie['MOVING_INC']
                dvizhenietotall['TOTAL_INC_ALL'] = dvizhenietotall['TOTAL_INC_ALL'] + dvizhenie['TOTAL_INC_ALL']
                dvizhenietotall['SALE_OUT'] = dvizhenietotall['SALE_OUT'] + dvizhenie['SALE_OUT']
                dvizhenietotall['OTHER_OUT'] = dvizhenietotall['OTHER_OUT'] + dvizhenie['OTHER_OUT']
                dvizhenietotall['SUPLRET_OUT'] = dvizhenietotall['SUPLRET_OUT'] + dvizhenie['SUPLRET_OUT']
                dvizhenietotall['MOVING_OUT'] = dvizhenietotall['MOVING_OUT'] + dvizhenie['MOVING_OUT']
                dvizhenietotall['TOTAL_OUT'] = dvizhenietotall['TOTAL_OUT'] + dvizhenie['TOTAL_OUT']
                dvizhenietotall['REST_END'] = dvizhenietotall['REST_END'] + dvizhenie['REST_END']
                arr.append(arrpi)
                a = 1
            arr.append(dvizhenietotallcode)
        arr.append(dvizhenietotall)
        dic['org_movement'] = arr
        arr = []
        shops = self.execute_sql("select * from rbs_list_objects(?,?,?,?,?,?)",
                                 sql_params=[org_id, None, 'SHOPS', None, None, None],
                                 fetch="all",
                                 db_local=self.db_conn)
        if shops['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения маназинов.' + c.kr_term_enter)
            self.errors.append('Ошибка получения маназинов.')
            return False

        for shop in shops['datalist']:
            org_info = self.execute_sql("select FULLNAME, INN, KPP, ADDRESS from GETOBJECTFIELDS(?)",
                                        sql_params=[shop['OBJID']],
                                        fetch="one",
                                        db_local=self.db_conn)
            if org_info['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения информации по маназину.' + c.kr_term_enter)
                self.errors.append('Ошибка получения информации по маназину.')
                return False
            org_info = org_info['datalist']
            if org_info['INN']:
                shop_inn = org_info['INN']
            else:
                shop_inn = org_inn
            shopinfo = {}
            shopinfo['STR'] = 'По обособленному подразделению: ' + self.coalesce(
                org_info['FULLNAME']) + ' ИНН/КПП: ' + self.coalesce(shop_inn) + \
                              '/' + self.coalesce(org_info['KPP']) + ' Адрес: ' + self.coalesce(org_info['ADDRESS'])
            arr.append(shopinfo)
            org_wareskind_oborot = self.execute_sql("select * from MY_AD_GET_TURNOVER(?,?,?,?)",
                                                    sql_params=[shop['OBJID'], form, d_beg, d_end],
                                                    fetch="all",
                                                    db_local=self.db_conn)
            if org_wareskind_oborot['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения select * from MY_AD_GET_TURNOVER(?,?,?,?).' + c.kr_term_enter)
                self.errors.append('Ошибка получения select * from MY_AD_GET_TURNOVER(?,?,?,?)')
                return False
            org_wareskind_oborot = org_wareskind_oborot['datalist']

            dvizhenietotall = {}
            dvizhenietotall['STR'] = 'ИТОГО:'
            dvizhenietotall['REST_BEG'] = 0
            dvizhenietotall['PRODUCTION_INC'] = 0
            dvizhenietotall['WHOLESALE_INC'] = 0
            dvizhenietotall['IMPORT_INC'] = 0
            dvizhenietotall['TOTAL_INC'] = 0
            dvizhenietotall['RET_INC'] = 0
            dvizhenietotall['OTHER_INC'] = 0
            dvizhenietotall['MOVING_INC'] = 0
            dvizhenietotall['TOTAL_INC_ALL'] = 0
            dvizhenietotall['SALE_OUT'] = 0
            dvizhenietotall['OTHER_OUT'] = 0
            dvizhenietotall['SUPLRET_OUT'] = 0
            dvizhenietotall['MOVING_OUT'] = 0
            dvizhenietotall['TOTAL_OUT'] = 0
            dvizhenietotall['REST_END'] = 0
            for wareskind in org_wareskind_oborot:
                wareskindarr = {}
                proizvoditel_importer = self.execute_sql("select * from my_ad_get_prod_importer(?,?,?,?)",
                                                         sql_params=[shop['OBJID'], d_beg, d_end, wareskind['WARESKINDID']],
                                                         fetch="all",
                                                         db_local=self.db_conn)
                if proizvoditel_importer['status'] == c.kr_sql_error:
                    self.log_file('Ошибка получения производителя/импортера.' + c.kr_term_enter)
                    self.errors.append('Ошибка получения производителя/импортера.')
                    return False
                proizvoditel_importer = proizvoditel_importer['datalist']

                wareskindarr['CODE'] = wareskind['CODE']
                wareskindarr['NAME'] = wareskind['NAME']

                dvizhenietotallcode = {}
                dvizhenietotallcode['STR'] = 'Итого по коду: ' + wareskindarr['CODE']
                dvizhenietotallcode['REST_BEG'] = 0
                dvizhenietotallcode['PRODUCTION_INC'] = 0
                dvizhenietotallcode['WHOLESALE_INC'] = 0
                dvizhenietotallcode['IMPORT_INC'] = 0
                dvizhenietotallcode['TOTAL_INC'] = 0
                dvizhenietotallcode['RET_INC'] = 0
                dvizhenietotallcode['OTHER_INC'] = 0
                dvizhenietotallcode['MOVING_INC'] = 0
                dvizhenietotallcode['TOTAL_INC_ALL'] = 0
                dvizhenietotallcode['SALE_OUT'] = 0
                dvizhenietotallcode['OTHER_OUT'] = 0
                dvizhenietotallcode['SUPLRET_OUT'] = 0
                dvizhenietotallcode['MOVING_OUT'] = 0
                dvizhenietotallcode['TOTAL_OUT'] = 0
                dvizhenietotallcode['REST_END'] = 0

                for pi in proizvoditel_importer:
                    arrpi = {}
                    dvizhenie = self.execute_sql("select * from my_ad_get_movement(?,?,?,?,?)",
                                                 sql_params=[shop['OBJID'], wareskind['WARESKINDID'], pi['ID'], d_beg,
                                                             d_end],
                                                 fetch="one",
                                                 db_local=self.db_conn)
                    if dvizhenie['status'] == c.kr_sql_error:
                        self.log_file('Ошибка получения движения.' + c.kr_term_enter)
                        self.errors.append('Ошибка получения движения.')
                        return False
                    dvizhenie = dvizhenie['datalist']

                    arrpi['IMPNAME'] = pi['FULLNAME']
                    arrpi['IMPINN'] = pi['INN']
                    arrpi['IMPKPP'] = pi['KPP']
                    arrpi['STR'] = ''
                    arrpi['wareskind'] = wareskindarr
                    arrpi['movement'] = dvizhenie

                    dvizhenietotallcode['REST_BEG'] = dvizhenietotallcode['REST_BEG'] + dvizhenie['REST_BEG']
                    dvizhenietotallcode['PRODUCTION_INC'] = dvizhenietotallcode['PRODUCTION_INC'] + dvizhenie[
                        'PRODUCTION_INC']
                    dvizhenietotallcode['WHOLESALE_INC'] = dvizhenietotallcode['WHOLESALE_INC'] + dvizhenie[
                        'WHOLESALE_INC']
                    dvizhenietotallcode['IMPORT_INC'] = dvizhenietotallcode['IMPORT_INC'] + dvizhenie['IMPORT_INC']
                    dvizhenietotallcode['TOTAL_INC'] = dvizhenietotallcode['TOTAL_INC'] + dvizhenie['TOTAL_INC']
                    dvizhenietotallcode['RET_INC'] = dvizhenietotallcode['RET_INC'] + dvizhenie['RET_INC']
                    dvizhenietotallcode['OTHER_INC'] = dvizhenietotallcode['OTHER_INC'] + dvizhenie['OTHER_INC']
                    dvizhenietotallcode['MOVING_INC'] = dvizhenietotallcode['MOVING_INC'] + dvizhenie['MOVING_INC']
                    dvizhenietotallcode['TOTAL_INC_ALL'] = dvizhenietotallcode['TOTAL_INC_ALL'] + dvizhenie[
                        'TOTAL_INC_ALL']
                    dvizhenietotallcode['SALE_OUT'] = dvizhenietotallcode['SALE_OUT'] + dvizhenie['SALE_OUT']
                    dvizhenietotallcode['OTHER_OUT'] = dvizhenietotallcode['OTHER_OUT'] + dvizhenie['OTHER_OUT']
                    dvizhenietotallcode['SUPLRET_OUT'] = dvizhenietotallcode['SUPLRET_OUT'] + dvizhenie[
                        'SUPLRET_OUT']
                    dvizhenietotallcode['MOVING_OUT'] = dvizhenietotallcode['MOVING_OUT'] + dvizhenie['MOVING_OUT']
                    dvizhenietotallcode['TOTAL_OUT'] = dvizhenietotallcode['TOTAL_OUT'] + dvizhenie['TOTAL_OUT']
                    dvizhenietotallcode['REST_END'] = dvizhenietotallcode['REST_END'] + dvizhenie['REST_END']

                    dvizhenietotall['REST_BEG'] = dvizhenietotall['REST_BEG'] + dvizhenie['REST_BEG']
                    dvizhenietotall['PRODUCTION_INC'] = dvizhenietotall['PRODUCTION_INC'] + dvizhenie[
                        'PRODUCTION_INC']
                    dvizhenietotall['WHOLESALE_INC'] = dvizhenietotall['WHOLESALE_INC'] + dvizhenie['WHOLESALE_INC']
                    dvizhenietotall['IMPORT_INC'] = dvizhenietotall['IMPORT_INC'] + dvizhenie['IMPORT_INC']
                    dvizhenietotall['TOTAL_INC'] = dvizhenietotall['TOTAL_INC'] + dvizhenie['TOTAL_INC']
                    dvizhenietotall['RET_INC'] = dvizhenietotall['RET_INC'] + dvizhenie['RET_INC']
                    dvizhenietotall['OTHER_INC'] = dvizhenietotall['OTHER_INC'] + dvizhenie['OTHER_INC']
                    dvizhenietotall['MOVING_INC'] = dvizhenietotall['MOVING_INC'] + dvizhenie['MOVING_INC']
                    dvizhenietotall['TOTAL_INC_ALL'] = dvizhenietotall['TOTAL_INC_ALL'] + dvizhenie['TOTAL_INC_ALL']
                    dvizhenietotall['SALE_OUT'] = dvizhenietotall['SALE_OUT'] + dvizhenie['SALE_OUT']
                    dvizhenietotall['OTHER_OUT'] = dvizhenietotall['OTHER_OUT'] + dvizhenie['OTHER_OUT']
                    dvizhenietotall['SUPLRET_OUT'] = dvizhenietotall['SUPLRET_OUT'] + dvizhenie['SUPLRET_OUT']
                    dvizhenietotall['MOVING_OUT'] = dvizhenietotall['MOVING_OUT'] + dvizhenie['MOVING_OUT']
                    dvizhenietotall['TOTAL_OUT'] = dvizhenietotall['TOTAL_OUT'] + dvizhenie['TOTAL_OUT']
                    dvizhenietotall['REST_END'] = dvizhenietotall['REST_END'] + dvizhenie['REST_END']
                    arr.append(arrpi)

                arr.append(dvizhenietotallcode)
            arr.append(dvizhenietotall)
        dic['shops'] = arr
        file_name = os.path.join(self.path_file, self.layer_code, 'alcodecl', file_name + '.xls')
        text = self.load_tmpl(tmpl, dic)
        self.export_file(file_name, text)

        return file_name

    # формирование excel файла алкодекларации разделII
    def alco_decl_formII(self, form, org_id, year, period, d_beg, d_end, ignoreerror=None):
        if form == 'R1II':
            form = 'R1'
            tmpl = 'formII_11.lb'
        elif form == 'R2II':
            form = 'R2'
            tmpl = 'formII_12.lb'
        else:
            self.errors('Не указана форма')
            return False

        d_beg += year
        d_end += year + ' 23:59:59'

        org_info = self.execute_sql("select FULLNAME, INN, KPP, ADDRESS from GETOBJECTFIELDS(?)",
                                    sql_params=[org_id],
                                    fetch="one",
                                    db_local=self.db_conn)

        if org_info['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения ИНН организации.' + c.kr_term_enter)
            self.errors.append('Ошибка получения ИНН организации.')
            return False
        org_info = org_info['datalist']

        if not org_info['INN']:
            self.errors.append('ИНН организации не указан')
            return False

        file_name = '_'.join([form, org_info['INN'], period + year[-1:],
                              self.date.replace('.', ''), str(uuid.uuid1()).upper()])

        org_inn = org_info['INN']
        dic = {}
        dic['org_info'] = org_info
        org_wareskind_oborot = self.execute_sql("select * from MY_AD_GET_TURNOVER_ORG(?,?,?,?)",
                                                sql_params=[org_id, form, d_beg, d_end],
                                                fetch="all",
                                                db_local=self.db_conn)
        if org_wareskind_oborot['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения select * from MY_AD_GET_TURNOVER_ORG(?,?,?,?).' + c.kr_term_enter)
            self.errors.append('Ошибка получения select * from MY_AD_GET_TURNOVER_ORG(?,?,?,?).')
            return False
        org_wareskind_oborot = org_wareskind_oborot['datalist']
        arr = []
        totallorg = {}
        totallorg['STR'] = 'ИТОГО:'
        totallorg['VALUE'] = 0
        for wareskind in org_wareskind_oborot:
            wareskindarr = {}
            proizvoditel_importer = self.execute_sql("select * from my_ad_get_prod_importer_org(?,?,?,?)",
                                                     sql_params=[org_id, d_beg, d_end, wareskind['WARESKINDID']],
                                                     fetch="all",
                                                     db_local=self.db_conn)
            if proizvoditel_importer['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения производитель/импортер.' + c.kr_term_enter)
                self.errors.append('Ошибка получения производитель/импортер.')
                return False
            proizvoditel_importer = proizvoditel_importer['datalist']

            wareskindarr['CODE'] = wareskind['CODE']
            wareskindarr['NAME'] = wareskind['NAME']
            for pi in proizvoditel_importer:

                postavschic = self.execute_sql("select * from my_ad_get_suppl_org(?,?,?,?,?)",
                                               sql_params=[org_id, wareskind['WARESKINDID'], pi['ID'],
                                                           d_beg, d_end.split(' ')[0]],
                                               fetch="all",
                                               db_local=self.db_conn)
                if postavschic['status'] == c.kr_sql_error:
                    self.log_file('Ошибка получения поставщика.' + c.kr_term_enter)
                    self.errors.append('Ошибка получения поставщика.')
                    return False
                postavschic = postavschic['datalist']
                totallcode = {}
                totallcode['STR'] = 'Итого по производителю ' + pi['FULLNAME'] + ' и коду: ' + wareskindarr['CODE']
                totallcode['VALUE'] = 0
                flag_isinc = False
                for post in postavschic:
                    postavschic_product = self.execute_sql("select * from my_ad_get_suppl_product_org(?,?,?,?,?,?,?)",
                                                           sql_params=[org_id, wareskind['WARESKINDID'], pi['ID'],
                                                                       post['SUPPLID'], post['SUPPLID'], d_beg, d_end],
                                                           fetch="all",
                                                           db_local=self.db_conn)
                    if postavschic_product['status'] == c.kr_sql_error:
                        self.log_file('Ошибка получения поставщика продукции.' + c.kr_term_enter)
                        self.errors.append('Ошибка получения поставщика продукции.')
                        return False
                    postavschic_product = postavschic_product['datalist']

                    for post_prod in postavschic_product:
                        arrpi = {}
                        arrpi['postavschic'] = post
                        arrpi['IMPNAME'] = pi['FULLNAME']
                        arrpi['IMPINN'] = pi['INN']
                        arrpi['IMPKPP'] = pi['KPP']
                        arrpi['wareskind'] = wareskindarr
                        arrpi['post_prod'] = post_prod
                        arr.append(arrpi)
                        flag_isinc = True
                        totallorg['VALUE'] = totallorg['VALUE'] + post_prod['pvolume']
                        totallcode['VALUE'] = totallcode['VALUE'] + post_prod['pvolume']
                if flag_isinc:
                    arr.append(totallcode)
        arr.append(totallorg)
        dic['org_data'] = arr
        arr = []
        shops = self.execute_sql("select * from rbs_list_objects(?,?,?,?,?,?)",
                                 sql_params=[org_id, None, 'SHOPS', None, None, None],
                                 fetch="all",
                                 db_local=self.db_conn)
        if shops['status'] == c.kr_sql_error:
            self.log_file('Ошибка получения магазинов.' + c.kr_term_enter)
            self.errors.append('Ошибка получения магазинов.')
            return False

        for shop in shops['datalist']:
            org_info = self.execute_sql("select FULLNAME, INN, KPP, ADDRESS from GETOBJECTFIELDS(?)",
                                        sql_params=[shop['OBJID']],
                                        fetch="one",
                                        db_local=self.db_conn)
            if org_info['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения информации по магазину.' + c.kr_term_enter)
                self.errors.append('Ошибка получения информации по магазину.')
                return False
            org_info = org_info['datalist']

            shopinfo = {}
            if (org_info['INN']):
                shop_inn = org_info['INN']
            else:
                shop_inn = org_inn
            shopinfo['STR'] = 'По обособленному подразделению: ' + self.coalesce(
                org_info['FULLNAME']) + ' ИНН/КПП: ' + self.coalesce(shop_inn) + \
                              '/' + self.coalesce(org_info['KPP']) + ' Адрес: ' + self.coalesce(org_info['ADDRESS'])
            arr.append(shopinfo)
            org_wareskind_oborot = self.execute_sql("select * from MY_AD_GET_TURNOVER(?,?,?,?)",
                                                    sql_params=[shop['OBJID'], form, d_beg, d_end],
                                                    fetch="all",
                                                    db_local=self.db_conn)
            if org_wareskind_oborot['status'] == c.kr_sql_error:
                self.log_file('Ошибка получения select * from MY_AD_GET_TURNOVER(?,?,?,?).' + c.kr_term_enter)
                self.errors.append('Ошибка получения select * from MY_AD_GET_TURNOVER(?,?,?,?).')
                return False
            org_wareskind_oborot = org_wareskind_oborot['datalist']

            totallorg = {}
            totallorg['STR'] = 'ИТОГО:'
            totallorg['VALUE'] = 0
            for wareskind in org_wareskind_oborot:
                wareskindarr = {}
                proizvoditel_importer = self.execute_sql("select * from my_ad_get_prod_importer(?,?,?,?)",
                                                         sql_params=[shop['OBJID'], d_beg, d_end, wareskind['WARESKINDID']],
                                                         fetch="all",
                                                         db_local=self.db_conn)
                if proizvoditel_importer['status'] == c.kr_sql_error:
                    self.log_file('Ошибка получения производителя/импортера.' + c.kr_term_enter)
                    self.errors.append('Ошибка получения производителя/импортера.')
                    return False
                proizvoditel_importer = proizvoditel_importer['datalist']

                wareskindarr['CODE'] = wareskind['CODE']
                wareskindarr['NAME'] = wareskind['NAME']
                for pi in proizvoditel_importer:

                    postavschic = self.execute_sql("select * from my_ad_get_suppl(?,?,?,?,?)",
                                                   sql_params=[shop['OBJID'], wareskind['WARESKINDID'], pi['ID'],
                                                               d_beg, d_end.split(' ')[0]],
                                                   fetch="all",
                                                   db_local=self.db_conn)
                    if postavschic['status'] == c.kr_sql_error:
                        self.log_file('Ошибка получения поставщика.' + c.kr_term_enter)
                        self.errors.append('Ошибка получения поставщика.')
                        return False
                    postavschic = postavschic['datalist']

                    totallcode = {}
                    totallcode['STR'] = 'Итого по производителю ' + pi['FULLNAME'] + ' и коду: ' + wareskindarr[
                        'CODE']
                    totallcode['VALUE'] = 0
                    flag_isinc = False
                    for post in postavschic:
                        postavschic_product = self.execute_sql("select * from my_ad_get_suppl_product(?,?,?,?,?,?,?)",
                                                               sql_params=[shop['OBJID'], wareskind['WARESKINDID'],
                                                                           pi['ID'],post['SUPPLID'], post['SUPPLID'],
                                                                           d_beg, d_end],
                                                               fetch="all",
                                                               db_local=self.db_conn)
                        if postavschic_product['status'] == c.kr_sql_error:
                            self.log_file('Ошибка получения поставщика продукции.' + c.kr_term_enter)
                            self.errors.append('Ошибка получения поставщика продукции.')
                            return False
                        postavschic_product = postavschic_product['datalist']

                        for post_prod in postavschic_product:
                            arrpi = {}
                            arrpi['postavschic'] = post
                            arrpi['IMPNAME'] = pi['FULLNAME']
                            arrpi['IMPINN'] = pi['INN']
                            arrpi['IMPKPP'] = pi['KPP']
                            arrpi['wareskind'] = wareskindarr
                            arrpi['post_prod'] = post_prod
                            arr.append(arrpi)
                            flag_isinc = True
                            totallorg['VALUE'] = totallorg['VALUE'] + post_prod['pvolume']
                            totallcode['VALUE'] = totallcode['VALUE'] + post_prod['pvolume']
                    if flag_isinc:
                        arr.append(totallcode)
            arr.append(totallorg)
        dic['shops'] = arr
        file_name = os.path.join(self.path_file, self.layer_code, 'alcodecl', file_name + '.xls')
        text = self.load_tmpl(tmpl, dic)
        self.export_file(file_name, text)
        return file_name

    def export_file(self, file_name, doc_txt, is_json=False):
        """
        Создание файла
        @param file_name: имя файла
        @param doc_txt: dict документа
        """

        self.create_folder(file_name)
        self.log_file('Создаем файл:' + file_name, terms=1)
        file_name = file_name.decode('cp1251')
        self.delete_tmp_file(file_name)
        if is_json:
            doc_txt = decodeUStr(json.dumps(doc_txt, encoding='cp1251', indent=1).encode('cp1251'))
        file_save = open(file_name, "a")
        print(doc_txt, file=file_save)
        file_save.close()

    def load_tmpl(self, file_name, data_dict):
        """
        Загрузка и исполнение шаблона
        @param file_name: имя шаблона
        @param data_dict: данные
        @return:
        """

        out_data = {}
        f = open('report/' + file_name)
        out_data['html'] = '\n\t' + f.read().replace('<', '\n\t<').replace('#', '\n\t#')
        for itm in data_dict:
            out_data[itm] = data_dict[itm]
        # out_data['html'] = '\n\t' + report_config.get('html', 'html').replace('<', '\n\t<').replace('#', '\n\t#')
        out_data['html'] = out_data['html']#.decode('utf8').encode('cp1251')
        tmpl_report = self.parent.k_conf.global_tmpl_report_alcodecl
        report_full_file_name = self.parent.k_conf.global_dir_report + '.' + tmpl_report
        exec ('from %s import %s' % (report_full_file_name, tmpl_report))
        html_report = str(locals()[tmpl_report](searchList=[out_data]))

        return html_report

