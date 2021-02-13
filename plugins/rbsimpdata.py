# -*- coding: utf-8 -*-
"""
    swat 15.01.2014
    version 0.0.2.0
    модуль импорта данных (объектов, товаров) из внешних систем
    базовый модуль
"""

import re
import os
import datetime
import time

import krconst
import krconst as c
import BasePlugin as BP

from rbsqutils import BarcodeToDic, translit_to_ident, unpack_file, convert1cxml
from rbsqutils import empty_str_to_null
from rbsqutils import str_to_bool_int

import plugins.impdata.object.impcategory as imp_cat
import plugins.impdata.object.impman as imp_man
import plugins.impdata.object.impuser as imp_user
import plugins.impdata.object.impbank as imp_bank
import plugins.impdata.object.impfirms as imp_firm
import plugins.impdata.object.impdepartmens as imp_dep
import plugins.impdata.object.impwarehouses as imp_warehouse
import plugins.impdata.object.impcustomers as imp_customer
import plugins.impdata.object.impmakers as imp_maker

import plugins.impdata.impsegment as imp_segment
import plugins.impdata.imprecipe as imp_recipe
import plugins.impdata.impcomponent as imp_component
import plugins.impdata.impformat as imp_format

import plugins.impdata.gwares.impunit as imp_unit
import plugins.impdata.xml.gwares.impwgroup_xml as imp_wgroup
import plugins.impdata.gwares.impgwares as imp_gwares


class Plugin(BP.BasePlugin):
    """
        модуль импорта данных (объектов, товаров) из внешних систем
        базовый модуль
    """

    # коннекст к БД engine
    engine_connect = None

    '''
        переменные для настройки импортов
        которые будут получены из xml параметров
    '''

    '''
        разрешен ли импорт пользователей.
        по умолчанию не импортируем
    '''
    i_users = '0'
    user_ignore_name = None

    '''
        параметры для проверки импорта по группам
    '''
    check_w_group = '0'
    check_w_group_value = ''

    '''
        предварительная конветация файла в нужный формат
        в параметра указываем какую конвертацию производить
        по умолчанию
    '''
    convert = None
    # результат конвертации
    #xml_convert = None

    '''
        Параметры для для отправки почты
    '''
    email_send = '0'
    email_to_address = None
    email_text = ''

    ''' Наименование атрибута id в справониках-xml для связи с внешними системами '''
    xml_name_external_id = None
    ''' Наименование атрибута delete в справониках-xml для связи с внешними системами '''
    xml_name_delete_flag = 'deletemarker'''
    ''' Признак добавления товара с пустой характеристикой '''
    add_none_characteristic = '0'

    ''' Для справочника ед измерения, наименования атрибутов названия кор и полного наименования
        флаг импорта фактора из внешних систем
    '''
    xml_unit_short_name = None
    xml_unit_full_name = None
    unit_flag_factor = 'EXT'

    ''' Справочник товаров '''
    ''' Параметры импорта ГГХ '''
    create_ttx = '0'
    coef_ttx = 1

    ''' Проверка на ассортимент товара '''
    gwares_check_assortment = '0'
    gwares_check_assortment_filter = ''

    ''' история НДС '''
    xml_tax_name_tag = 'ndsgwares'

    ''' создавать ассортимент + наборы'''
    gwares_create_assortment = '0'
    gwares_create_wset = '0'

    ''' импортировать тип товара '''
    gwares_create_type = '0'

    ''' импортировать МПО товара '''
    gwares_create_mpo = '0'

    ''' импорт сезонности товара '''
    gwares_create_season = '0'

    ''' импорт прайслистов '''
    gwares_create_price_list = '0'

    ''' параметр из какой системы берутся сроки годности '''
    gwares_ubd = 'WH'

    ''' Путь для экспорта файла оповещения'''
    path_confirm_mes = None

    # номер мообщения
    message_number = None

    def run(self):
        # проверим формат файла
        if (os.path.basename(self.filenames)).split('.')[1] in ('7z', 'zip'):
            # распакуем файл
            res_unpack_file = unpack_file(self.filenames)
            if res_unpack_file['file_name']:
                self.filenames = res_unpack_file['file_name']
            else:
                self.result['result'] = krconst.plugin_error
                message = krconst.m_e_unpack_file % self.filenames
                self.log_file(message,
                              terms = 2,
                              save_log_db=True)
                return False

        ''' получение параметров работы плангина '''
        self.get_params_queue()

        # конвертация файла при необходимости
        self.file_convert()

        xmlfile = self.ParseFileXML(self.filenames)
        if self.result['result'] == krconst.plugin_error:
            return False

        # пробуем получить номер пакета с данными
        try:
            self.message_number = xmlfile.getroot().attrib['messageNumber']
        except:
            self.message_number = None

        self.createshedule = '0'
        self.createuser = '0'
        self.createunits = '1'
        self.createPromo = '1'
        self.syncextorderrestrict = '0'

        if not self.taskparamsxml and self.taskparamsxml == '':
            self.result['result'] = krconst.plugin_error
            self.log_file(krconst.m_e_emptytaskparams,
                          terms = 2,
                          save_log_db=True)
            return False

        if self.message_number:
            sql_text = 'execute procedure RBS_Q_IMPORT_NUMBER_INS(?,?)'
            sql_params = [self.queueid, int(self.message_number)]
            res = self.execute_sql(sql_text,
                                   sql_params=sql_params,
                                   fetch='none')
            if res['status'] == krconst.kr_sql_error:
                self.log_file('Ошибка сохранения номера пакета', terms=1)
                return False

        # проверим наличие параметра createshedule
        self.createshedule = self.ParserXML(self.taskparamsxml, 'createshedule')
        if not self.createshedule:
            self.createshedule = '0'

        # проверим наличие параметра createuser
        self.createuser = self.ParserXML(self.taskparamsxml, 'createuser')
        if not self.createuser:
            self.createuser = '0'

        # проверим наличие параметра createunits
        self.createunits = self.ParserXML(self.taskparamsxml, 'createunits')
        if not self.createunits:
            self.createunits = '1'

        # проверим наличие параметра createPromo
        self.createPromo = self.ParserXML(self.taskparamsxml, 'createPromo')
        if not self.createPromo:
            self.createPromo = '1'

        # проверим наличие параметра syncextorderrestrict
        self.syncextorderrestrict = self.ParserXML(self.taskparamsxml, 'syncextorderrestrict')
        if not self.syncextorderrestrict:
            self.syncextorderrestrict = '0'

        ## Для импорта справочника объектов

        ''' импорт форматов '''
        formats = xmlfile.find('formats')
        if formats is not None:
            self.import_formats(formats)

        ''' импорт банков '''
        banks = xmlfile.find('banks')
        if banks is not None:
            self.import_banks(banks)

        '''  импорт контрагентов '''
        customers = xmlfile.find('customers')
        if customers is not None:
            self.import_customers(customers)

        '''  импорт производителей '''
        makers = xmlfile.find('makers')
        if makers is not None:
            self.import_makers(makers)

        ''' импорт должностей '''
        dolgns = xmlfile.find('dolgns')
        if dolgns is not None:
            self.import_dolgns(dolgns)

        ''' импорт сотрудников '''
        mans = xmlfile.find('mans')
        if mans is not None:
            self.import_mans(mans)

        # импорт пользователей
        users = xmlfile.find('users')
        if users is not None:
            self.import_users(users)

        ''' импорт подразделений '''
        departmens = xmlfile.find('departmens')
        if departmens is not None:
            self.import_departmens(departmens)

        ''' импорт складов '''
        warehouses = xmlfile.find('warehouses')
        if warehouses is not None:
            self.import_warehouses(warehouses)

        ''' фирмы '''
        firms = xmlfile.find('firms')
        if firms is not None:
            self.import_firms(firms)

        ## Для импорта справочника товаров
        # импорт периодов
        periods = xmlfile.find('periods')
        if periods is not None:
            self.ImportPeriods(periods)

        # импорт налоговых ставок
        taxs = xmlfile.find('taxs')
        if taxs is not None:
            self.ImportTaxs(taxs)

        ''' импорт справочника ед измерения '''
        if self.createunits == '1':
            units = xmlfile.find('units')
            if units is not None:
                self.import_units(units)

        # импорт товаров
        gwares = xmlfile.find('gwares')
        if gwares is not None:
            self.import_gwares(gwares)

        if self.createshedule == '1' and customers is not None:
            self.CreateShedule(customers)

        #импорт акций
        if self.createPromo == '1':
            promos = xmlfile.find('promos')
            if promos is not None:
                self.ImportPromos(promos)

        # импорт пользователей
        if self.createuser == '1':
            groupmenegers = xmlfile.find('groupmenegers')
            if groupmenegers is not None:
                self.ImportGroupMenegers(groupmenegers)

        # импорт готовых наборов
        wsets = xmlfile.find('wsets')
        if wsets is not None:
            self.ImportWsets(wsets)

        # импорт прайс листов
        if self.gwares_create_price_list == '1':
            pricelists = xmlfile.find('pricelist')
            if pricelists is not None:
                self.import_price_list(pricelists)

        # импорт (дополнительно) ед измерений товаров
        gwaresunits = xmlfile.find('gwaresunits')
        if gwaresunits is not None:
            self.import_gwares_units(gwaresunits)

        # импорт договоров
        contracts = xmlfile.find('contracts')
        if contracts is not None:
            self.import_contracts(contracts)

        # импорт сегмента
        segments = xmlfile.find('segments')
        if segments is not None:
            self.import_segment(segments)

        # импорт рецепта
        recipes = xmlfile.find('recipes')
        if recipes is not None:
            self.import_recipe(recipes)

        # импорт алкогольного справочника
        alcotypes = xmlfile.find('alcotypes')
        if alcotypes is not None:
            self.import_alcotypes(alcotypes)

        # файл маркер, если есть номер пакета
        if 'confirm_mes' in self.filenames:
            # вызов процедур для запретов
            # заказ
            sql = 'execute procedure RBS_Q_ASSORTMENT_SYNCEXTORDREST'
            res = self.execute_sql(sql)
            if res['status'] == c.kr_sql_error:
                self.LogFile('Ошибка вызова процедуры RBS_Q_ASSORTMENT_SYNCEXTORDREST', Terms=2)
            # продажа
            sql = 'execute procedure RBS_Q_ASSORTMENT_SYNCSALEREST'
            res = self.execute_sql(sql)
            if res['status'] == c.kr_sql_error:
                self.LogFile('Ошибка вызова процедуры RBS_Q_ASSORTMENT_SYNCSALEREST', Terms=2)

            v8msg = 'http://v8.1c.ru/messages'
            Header = xmlfile.find('{' + v8msg + '}Header')
            ExchangePlan = Header.find('{' + v8msg + '}ExchangePlan').text
            To = Header.find('{' + v8msg + '}To').text
            From = Header.find('{' + v8msg + '}From').text
            MessageNo = Header.find('{' + v8msg + '}MessageNo').text
            ReceivedNo = Header.find('{' + v8msg + '}ReceivedNo').text

            # деалаем файл ответ
            xml_text = '<?xml version="1.0" encoding="UTF-8"?>' + '\n'
            xml_text += '<v8msg:Message xmlns:v8msg="http://v8.1c.ru/messages">' + '\n'
            xml_text += '    <v8msg:Header>' + '\n'
            xml_text += '        <v8msg:ExchangePlan>' + ExchangePlan + '</v8msg:ExchangePlan>' + '\n'
            xml_text += '        <v8msg:To>' + From + '</v8msg:To>' + '\n'
            xml_text += '        <v8msg:From>' + To + '</v8msg:From>' + '\n'
            xml_text += '        <v8msg:MessageNo>' + str(int(ReceivedNo) + 1) + '</v8msg:MessageNo>' + '\n'
            xml_text += '        <v8msg:ReceivedNo>' + MessageNo + '</v8msg:ReceivedNo>' + '\n'
            xml_text += '    </v8msg:Header>' + '\n'
            xml_text += '    <v8msg:Body/>' + '\n'
            xml_text += '</v8msg:Message>' + '\n'

            # формирование имени файла
            now = datetime.datetime.now()
            s1 = 'temp_confirm_mes_'
            s2 = time.strftime('%Y%m%d%H%M%S', time.localtime()) + str(now.microsecond)

            #file_name_temp = '/base/share/1C/' + s1 + s2 + '.xml'
            #file_name = '/base/share/1C/confirm_mes_' + s2 + '.xml'
            file_name_temp = self.path_confirm_mes + s1 + s2 + '.xml'
            file_name = self.path_confirm_mes + 'confirm_mes_' + s2 + '.xml'

            ''' сохраняем сначала во временный файл '''
            try:
                self.text_save_to_file(xml_text.encode('utf-8', 'ignore'), file_name_temp)
            except:
                self.TracebackLog('Ошибка сохранения во временый файл')
                self.log_to_db('Ошибка сохранения во временый файл')

            ''' переименовываем временный файл в нормальный '''
            try:
                self.move_file(file_name_temp, file_name)
            except:
                self.TracebackLog('Ошибка переименования файла')
                self.LogDB('Ошибка переименования файла')

        if self.email_send == '1' and self.email_to_address:
            if self.email_text != '' or self.result['result'] == krconst.plugin_error:
                # создаем задание на отправку письма
                ext_text = 'ДанныеПоОбмену  '\
                            'Кому=&quot;%s&quot; ОтКого=&quot;%s&quot; '\
                            'НомерИсходящегоСообщения=&quot;%s&quot; '\
                            'НомерВходящегоСообщения=&quot;%s&quot; ' % \
                            (self.xml_convert['to'],
                             self.xml_convert['from'],
                             self.xml_convert['outsms'],
                             self.xml_convert['insms'])
                self.email_text = ext_text + '#1#1' + self.email_text
                sql_text = 'select * from RBS_Q_CREATEMAIL(?,?,?)'
                sql_params = [self.email_to_address, self.email_text, 'Ошибка импорта в систему RBS.']
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.log_file(krconst.m_e_createmail,
                                  terms=2,
                                  save_log_db=True)

    def get_params_queue(self):
        """
            Получение параметров работы
        """

        self.i_users = self.get_single_params('i_users', self.i_users)
        self.user_ignore_name = self.get_single_params('user_ignore_name', self.user_ignore_name)
        self.user_ignore_name = empty_str_to_null(self.user_ignore_name)

        self.convert = self.get_single_params('convert', self.convert)
        self.email_send = self.get_single_params('email_send', self.email_send)
        self.email_to_address = self.get_single_params('email_toaddress', self.email_to_address)

        self.check_w_group = self.get_single_params('check_w_group', self.check_w_group)
        self.check_w_group_value = self.get_single_params('check_w_group_value', self.check_w_group_value)

        self.xml_name_external_id = self.get_single_params('xml_name_external_id', self.xml_name_external_id)
        self.xml_name_delete_flag = self.get_single_params('xml_name_delete_flag', self.xml_name_delete_flag)
        self.add_none_characteristic = self.get_single_params('add_none_characteristic', self.add_none_characteristic)

        self.xml_unit_short_name = self.get_single_params('unitsshortname', self.xml_unit_short_name)
        self.xml_unit_full_name = self.get_single_params('unitsfullname', self.xml_unit_full_name)
        self.unit_flag_factor = self.get_single_params('unit_flag_factor', self.unit_flag_factor)

        self.create_ttx = self.get_single_params('creategwaresTTX', self.create_ttx)
        self.coef_ttx = int(self.get_single_params('gwaresTTXCoef', self.coef_ttx))

        self.gwares_check_assortment = self.get_single_params('checkGwaresHW', self.gwares_check_assortment)
        self.gwares_check_assortment_filter = self.get_single_params('checkGwaresHWSuppliers',
                                                                     self.gwares_check_assortment_filter)

        self.xml_tax_name_tag = self.get_single_params('nametaxtag', self.xml_tax_name_tag)

        self.gwares_create_assortment = self.get_single_params('gwares_create_assortment',
                                                               self.gwares_create_assortment)
        self.gwares_create_wset = self.get_single_params('gwares_create_wset',
                                                         self.gwares_create_wset)
        self.gwares_create_type = self.get_single_params('gwares_create_type', self.gwares_create_type)
        self.gwares_create_mpo = self.get_single_params('gwares_create_mpo', self.gwares_create_mpo)
        self.gwares_create_season = self.get_single_params('gwares_create_season', self.gwares_create_season)
        self.gwares_create_price_list = self.get_single_params('gwares_create_price_list',
                                                               self.gwares_create_price_list)

        self.gwares_ubd = self.get_single_params('gwares_ubd', self.gwares_ubd)

        self.path_confirm_mes = self.get_single_params('path_confirm_mes', self.path_confirm_mes)

    def get_single_params(self, name, default):
        """
            Получение точечного параметра,
            если его нет, то заполняется по
            умолчанию
        """

        value = self.ParserXML(self.taskparamsxml, name)
        if not value:
            value = default

        return value

    def file_convert(self):
        """
            Конвертация файла в нужный формат обмена
        """

        if self.convert:
            if self.convert == 'magnit_from_1c':
                self.xml_convert = convert1cxml(open(self.filenames).read())
                cur_date = datetime.datetime.now()
                str_cur_date = cur_date.strftime('%d%m%Y%H%M%S')
                file_dir = os.path.dirname(self.filenames)
                file_name = 'reference_data_' + str_cur_date + '.xml'
                self.filenames = os.path.join(file_dir, file_name)

                f = open(self.filenames, 'w')
                f.write(self.xml_convert['xml'])
                f.close()

    def get_connect_engine(self):
        """
            Получим коннект к БД Engine
        """

        db_code = 'ENGINE'

        db_cfg = self.read_config_other_db(db_code)
        if not db_cfg:
            self.log_file('Нет настроек доп БД!',
                          terms=2,
                          save_log_db=True)
            self.result['result'] = krconst.plugin_error
            return False
        else:
            self.engine_connect = self.connect_other_db(db_cfg)
            if not self.engine_connect:
                self.log_file('Нет подключения к доп БД!',
                              terms=2,
                              save_log_db=True)
                self.result['result'] = krconst.plugin_error
                return False

    def import_customers(self, customers):
        """
            Импорт контрагентов
        """

        for obj in customers:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                cat = imp_cat.Category(self, 'I', 'C', obj)
                cat.save()
            else:
                c = imp_customer.Customers(self, 'I', obj)
                c.save()
                c.save_wsetproducer_customer()
                c.save_bank_account()
                c.save_print_data()
                c.save_activity()

    def import_dolgns(self, dolgns):
        """
            Импорт должностей
        """

        pass
        '''
        for obj in dolgns:
            dolgn = impdolgn.Dolgn(self, obj)
            dolgn.save()
            '''

    def import_mans(self, mens):
        """
            Импорт сотрудников.
            Если включен флаг импорта пользователей,
            то сразу получим подключение к БД Engine
        """

        if self.i_users in ('1', '2'):
            self.get_connect_engine()

        for obj in mens:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                cat = imp_cat.Category(self, 'I', 'M', obj)
                cat.save()
            else:
                text = ''
                m = imp_man.Man(self, 'I', obj)
                if m.check_ignore_user():
                    if self.i_users in('1', '2'):
                        if self.i_users == '1':
                            dolgn_guid = self.xml_get_value_by_attr(obj, 'dolgnguid', flag='N')
                            if dolgn_guid:
                                m.save()
                                m.set_dolgn(obj)
                                if m.result_class == krconst.plugin_ok:
                                    user = imp_user.User(self, m.objid, m.name, None, m.dolgnid, self.engine_connect)
                                    user.save()
                                    if user.result_class == krconst.plugin_error:
                                        text = 'Импорт пользователя прерван %s . ' \
                                               'Техническая проблема с пользователем, смотрите лог файл.#1' % m.name
                                else:
                                    text = 'Импорт пользователя %s невозможен. ' \
                                           'Техническая проблема c физ лицом, смотрите лог файл.#1' % m.name
                            else:
                                text = 'Импорт физ лица %s невозможен, не уставновлена должность.#1' % m.name
                                self.write_message(text)
                        if self.i_users == '2':
                            m.save()
                            user = imp_user.User(self, m.objid, m.name, None, m.dolgnid, self.engine_connect)
                            user.save()
                    else:
                        m.save()
                if m.result_class == krconst.plugin_error:
                    text = 'Импорт физ лица прерван %s. Техническая проблема, смотрите лог файл..#1' % m.name
                if text != '':
                    self.write_message(text)

    def import_users(self, users):
        """
            импорт пользователей
        """

        # закоментировано для новой схемы
        # # установим пользователям флаг предимпорта
        # sql_params = []
        # sql_text = 'execute procedure RBS_Q_MAN_PREIMPORT'
        #
        # res = self.execute_sql(sql_text,
        #                        sql_params=sql_params,
        #                        fetch='none',
        #                        ext_ver=True)
        # if res['status'] == krconst.kr_sql_error:
        #     self.log_file('Ошибка предимпорта сотрудников', terms=1)
        # else:
        for obj in users:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                g = imp_wgroup.WGroupXML(self, obj)
                g.save()
            else:
                m = imp_man.Man(self, 'I', obj)
                m.save()
                m.set_email()
            # if self.result['result'] == c.plugin_ok:
            #     sql_params = []
            #     sql_text = 'execute procedure RBS_Q_MAN_AFTERIMPORT'
            #
            #     res = self.execute_sql(sql_text,
            #                            sql_params=sql_params,
            #                            fetch='none',
            #                            ext_ver=True)
            #     if res['status'] == krconst.kr_sql_error:
            #         self.log_file('Ошибка после импорта сотрудников', terms=1)

    def import_segment(self, segments):
        """
            Импорт сегмента
        """

        for obj in segments:
            segment = imp_segment.Segment(self, obj)
            segment.save()

    def import_recipe(self, recipes):
        """
            Импорт рецептов
        """

        for obj in recipes:
            is_success_imp_component = True
            recipe = imp_recipe.Recipe(self, obj)
            recipe.save()
            if recipe.recipe_id:
                wares = obj.find('wares')
                if wares is not None:
                    for itm in wares:
                        component = imp_component.Component(self, itm)
                        component.save(recipe.recipe_id)
                        if component.result_class == krconst.plugin_error:
                            is_success_imp_component = False
                if is_success_imp_component:
                    component.clear(recipe.recipe_id, '1')
                else:
                    component.clear(recipe.recipe_id, '0')

    def write_message(self, text):
        """
            Составление тела письма
        """

        if self.email_send == '1':
            if text:
                self.email_text = self.email_text + text

    def import_firms(self, firms):
        """
            Импорт фирм
        """

        for obj in firms:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                cat = imp_cat.Category(self, 'I', 'F', obj)
                cat.save()
            else:
                f = imp_firm.Firms(self, 'I', obj)
                f.save()

    def import_banks(self, banks):
        """
            Импорт банков
        """

        for obj in banks:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                cat = imp_cat.Category(self, 'I', 'B', obj)
                cat.save()
            else:
                b = imp_bank.Banks(self, 'I', obj)
                b.save()

    def import_warehouses(self, warehouses):
        """
            Импорт складов
        """

        for obj in warehouses:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                cat = imp_cat.Category(self, 'I', 'W', obj)
                cat.save()
            else:
                w = imp_warehouse.Warehouses(self, 'I', obj)
                w.save()

    def import_departmens(self, departmens):
        """
            Импорт подразделений
        """

        for obj in departmens:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                cat = imp_cat.Category(self, 'I', 'D', obj)
                cat.save()
            else:
                d = imp_dep.Departmens(self, 'I', obj)
                d.save()

    def import_makers(self, makers):
        """
            Импорт производителей
        """

        for obj in makers:
            p = imp_maker.Makers(self, 'I', obj)
            p.save()

    def import_alcotypes(self, alcotypes):
        """
            Импорт справочника Виды алкогольной продукции
        """

        for obj in alcotypes:
            code = self.xml_get_value_by_attr(obj, 'code')
            name = self.xml_get_value_by_attr(obj, 'name')
            type_name = self.xml_get_value_by_attr(obj, 'type')
            deletemarker = self.xml_get_value_by_attr(obj, 'deletemarker')
            deletemarker = str_to_bool_int(deletemarker)
            # тут используется как status нужно инвертировать
            if deletemarker == '1':
                deletemarker = '0'
            else:
                deletemarker = '1'

            sql_text = 'select * from RBS_Q_GWARES_ALCOTYPES_INSEL(?,?,?,?)'
            sql_params = [code, name, type_name, deletemarker]
            res = self.execute_sql(sql_text,
                                   sql_params = sql_params,
                                   fetch='one')
            if res['status'] == krconst.kr_sql_error:
                self.log_file('Ошибка импорта типа алкогольной продукции:' + code, terms=1)

    def ImportCategorySQL(self, sqlparams, codecat):
        """

        """

        res = self.ExecuteSQL('execute procedure RBS_Q_CATEGORY_INSSEL(?,?,?,?,?,?,?,?)',
                              sqlparams = sqlparams,
                              fetch='one',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_importcategory % codecat + krconst.kr_term_double_enter)

    def ImportCustomersSQL(self, sqlparasm, codeobj, typeobj, getObjid=False):
        """
            SQL импорт объектов
        """

        objid = None

        res = self.ExecuteSQL('select * from RBS_Q_OBJ_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                              sqlparams = sqlparasm,
                              fetch='one',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_i_object % codeobj + '. ' + krconst.kr_message_error_importtypeobject % typeobj +  krconst.kr_term_double_enter)
        else:
            if getObjid:
                objid = res['datalist']['OBJID']
        return objid

    def import_gwares(self, gwares):
        """
            Импорт товаров
        """

        for obj in gwares:
            if str_to_bool_int(self.xml_get_value_by_attr(obj, 'group')) == '1':
                g = imp_wgroup.WGroupXML(self, obj)
                g.save()
                if g.wgroup_id is not None and g.singularity is not None:
                    g.save_singularity()
                # return False
            else:
                w = imp_gwares.Gwares(self, obj)
                if w.is_import_wares:
                    w.save()
                    # return False
            if self.gwares_create_wset == 'A':
                self.create_wset()

        if not self.message_number:
            # вызов процедур для запретов только если это полный справочник
            # заказ
            sql = 'execute procedure RBS_Q_ASSORTMENT_SYNCEXTORDREST'
            res = self.execute_sql(sql)
            if res['status'] == c.kr_sql_error:
                self.LogFile('Ошибка вызова процедуры RBS_Q_ASSORTMENT_SYNCEXTORDREST', Terms=2)
            # продажа
            sql = 'execute procedure RBS_Q_ASSORTMENT_SYNCSALEREST'
            res = self.execute_sql(sql)
            if res['status'] == c.kr_sql_error:
                self.LogFile('Ошибка вызова процедуры RBS_Q_ASSORTMENT_SYNCSALEREST', Terms=2)

    def import_units(self, units):
        """
            импорт ед измерения
        """

        for obj in units:
            u = imp_unit.Unit(self, obj)
            u.save()

    def ImportTaxs(self, taxs):
        """
            Импорт налоговых ставок
        """

        for obj in taxs:
            codetax = self.xml_get_value_by_attr(obj, 'code')
            nametax = self.xml_get_value_by_attr(obj, 'name')
            rate = self.xml_get_value_by_attr(obj, 'rate', flag='N')
            if not rate:
                rate = 0
            res = self.ExecuteSQL('execute procedure RBS_Q_TAX_INSSEL(?,?,?,?)',
                                   sqlparams = [codetax, nametax, rate, 'I'],
                                   fetch='one',
                                   ExtVer=True)
            if res['status'] == krconst.kr_sql_error:
                self.LogFile(krconst.kr_message_error_importtax % codetax + krconst.kr_term_double_enter)

    def ImportPeriods(self, periods):
        """
            Импорт периодов
        """

        for obj in periods:
            codeperiod =  self.xml_get_value_by_attr(obj, 'code')
            nameperiod =  self.xml_get_value_by_attr(obj, 'name')
            typeperiod =  self.xml_get_value_by_attr(obj, 'type')
            dataperiod =  self.xml_get_value_by_attr(obj, 'data')
            res = self.ExecuteSQL('execute procedure RBS_Q_PERIOD_INSSEL(?,?,?,?,?)',
                                  sqlparams = [codeperiod, typeperiod, dataperiod, nameperiod, 'I'],
                                  fetch='one',
                                  ExtVer=True)
            if res['status'] == krconst.kr_sql_error:
                self.LogFile(krconst.kr_message_error_importperiod % codeperiod + krconst.kr_term_double_enter)

    def import_formats(self, formats):
        """
            Импорт периодов
        """

        for obj in formats:
            f = imp_format.Format(self, obj)
            f.save()

    def ImportGwaresSQL(self, paramssql, warescode, getID=False):
        """
            Импорт товаров SQL
        """

        resData = {}
        res = self.ExecuteSQL('execute procedure RBS_Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                              sqlparams = paramssql,
                              fetch='one',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_i_wares % warescode + krconst.kr_term_double_enter)
        if getID:
            resData['status'] = res['status']
        return res['status']

    def import_wares_barcode(self, warescode, unit, barcodes, externalid, create_ttx=None):
        """
            импорт ШК товаров
        """

        if create_ttx:
            self.create_ttx = create_ttx
        waresbarcode = []
        getunits = 1
        if unit:
            getunits = 0
        for barcode in barcodes:
            uweight = None
            ulength = None
            uheight = None
            uwidth = None

            factor = self.xml_get_value_by_attr(barcode, 'factor', flag='N')

            if getunits == 1:
                unit = self.xml_get_value_by_attr(barcode, 'unit')

            #if unit != "Удаленный":
            if self.create_ttx == '1':
                uweight = self.xml_get_value_by_attr(barcode, 'uweight', flag='N')
                ulength = self.xml_get_value_by_attr(barcode, 'ulength', flag='N')
                uheight = self.xml_get_value_by_attr(barcode, 'uheight', flag='N')
                uwidth = self.xml_get_value_by_attr(barcode, 'uwidth', flag='N')

            characteristic = self.xml_get_value_by_attr(barcode, 'characteristic', flag='N')
            waresbarcode = BarcodeToDic(waresbarcode, self.xml_get_value_by_attr(barcode, 'value'), unit, factor, uweight, ulength, uheight, uwidth, characteristic)

        if waresbarcode:
            for itm in waresbarcode:
                res = self.ExecuteSQL('select * from RBS_Q_IMP_WARESBARCODE(?,?,?,?,?,?,?,?,?,?,?)',
                                      sqlparams = [warescode, itm['unit'], itm['barcode'], itm['factor'],
                                                   itm['uweight'], itm['ulength'], itm['uheight'], itm['uwidth'],
                                                   self.coef_ttx, itm['characteristic'], externalid],
                                      fetch='one',
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_wares % warescode + '. ' + krconst.m_e_i_wares_barcode % itm['barcode'] + krconst.kr_term_double_enter)

    def ImportWareUnits(self, waresunits, warescode, type, externalid, secondaryname,create_ttx=None):
        """
            Импорт ед. измерения
        """

        for obj in waresunits:
            if self.xml_get_value_by_attr(obj, 'type') == type:
                unit = self.xml_get_value_by_attr(obj, 'unit')
                factor = self.xml_get_value_by_attr(obj, 'factor', flag='N')
                ulength = self.xml_get_value_by_attr(obj, 'ulength', flag='N')
                uwidth = self.xml_get_value_by_attr(obj, 'uwidth', flag='N')
                uheight = self.xml_get_value_by_attr(obj, 'uheight', flag='N')
                uweight = self.xml_get_value_by_attr(obj, 'uweight', flag='N')
                unetweight = self.xml_get_value_by_attr(obj, 'unetweight', flag='N')
                res = self.ExecuteSQL('select * from RBS_Q_WARESUNIT_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?)',
                                      sqlparams = [warescode, unit, factor, type, ulength, uwidth, uheight, uweight, unetweight, self.coef_ttx,
                                                    externalid, secondaryname],
                                      fetch='one',
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_wares % warescode + '. ' + krconst.m_e_i_wares_unit % unit + krconst.kr_term_double_enter)
                else:
                    # находим ШК
                    waresbarcode = []
                    barcodes = obj.find('barcodes')
                    if barcodes is not None:
                        self.import_wares_barcode(warescode, unit, barcodes, externalid, create_ttx)

    def create_wset(self):
        """
            Автосоздание наборов
        """

        sql_text = 'select * from RBS_GENERATE_WSETS(?,?)'
        sql_params = [None, None]

        res = self.ExecuteSQL(sql_text,
                              sqlparams = sql_params,
                              fetch='all',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_i_wset, Terms=2)
        return res['status']

    def CreateShedule(self, customers):
        """
            Импорт графиков автозаказа
        """

        for obj in customers:
            deliverytime = self.xml_get_value_by_attr(obj, 'deliverytime')
            code = self.xml_get_value_by_attr(obj, 'code')
            schedules = obj.find('schedules')
            if schedules is not None:
                for schedule in schedules:
                    days = self.xml_get_value_by_attr(schedule, 'days')
                    res = self.ExecuteSQL('execute procedure RBS_Q_INIT_SHEDULER(?,?,?)',
                                          sqlparams = [code, deliverytime, days],
                                          fetch='none',
                                          ExtVer=True)
                    if res['status'] == krconst.kr_sql_error:
                        self.LogFile(krconst.kr_message_error_importshedule % code, Terms=2)

    def ImportPromos(self, promos):
        """
            Импорт промоакций
        """

        for obj in promos:
            typepromo = self.xml_get_value_by_attr(obj, 'typepromo', flag='N')
            datestart = self.xml_get_value_by_attr(obj, 'datestart', flag='N')
            datestartpurchase = self.xml_get_value_by_attr(obj, 'datestartpurchase', flag='N')
            datefinish = self.xml_get_value_by_attr(obj, 'datefinish', flag='N')
            datefinishpurchase = self.xml_get_value_by_attr(obj, 'datefinishpurchase', flag='N')
            code = self.xml_get_value_by_attr(obj, 'code', flag='N')
            name = self.xml_get_value_by_attr(obj, 'name', flag='N')
            customercode = self.xml_get_value_by_attr(obj, 'customercode', flag='N')
            customername = self.xml_get_value_by_attr(obj, 'customer', flag='N')
            parent = self.xml_get_value_by_attr(obj, 'parent', flag='N')
            parentcode = self.xml_get_value_by_attr(obj, 'parentcode', flag='N')
            if parentcode == '0':
                parentcode = None
            if customercode == '0':
                customercode = None
            parentgroup = str_to_bool_int(self.xml_get_value_by_attr(obj, 'parentgroup', flag='N'))
            isgroup = str_to_bool_int(self.xml_get_value_by_attr(obj, 'group', flag='N'))
            deletemarker = str_to_bool_int(self.xml_get_value_by_attr(obj, 'deletemarker', flag='N'))

            sql_text = 'select OBJID, WSETID from RBS_SHARE_IMPORT_SHARES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            sql_params = [typepromo, datestart, datestartpurchase, datefinish, datefinishpurchase,
                          code, name, customercode, customername, None, parent, parentcode, parentgroup,
                          isgroup, deletemarker]
            if customercode:
                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='one',
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_promo % code, Terms=2)
                else:
                    if res['datalist']:
                        OBJID = res['datalist']['OBJID']
                        WSETID = res['datalist']['WSETID']
                        gwares = obj.find('gwares')
                        if gwares is not None:
                            for wares in gwares:
                                warescode = self.xml_get_value_by_attr(wares, 'warescode')
                                waresname = self.xml_get_value_by_attr(wares, 'waresname')
                                pricepurchase = self.xml_get_value_by_attr(wares, 'pricepurchase')
                                price = self.xml_get_value_by_attr(wares, 'price')
                                oldprice = self.xml_get_value_by_attr(wares, 'oldprice')

                                sql_text = 'execute procedure RBS_SHARE_IMPORT_SHARE_WARES(?,?,?,?,?,?,?)'
                                sql_params = [None, WSETID, warescode, waresname, pricepurchase, price, oldprice]
                                reswares = self.ExecuteSQL(sql_text,
                                                           sqlparams = sql_params,
                                                           fetch='none',
                                                           ExtVer=True)
                                if reswares['status'] == krconst.kr_sql_error:
                                    self.LogFile(krconst.m_e_i_promo_wares % warescode, Terms=2)
                        objects = obj.find('objects')
                        if objects is not None:
                            for shop in objects:
                                code = self.xml_get_value_by_attr(shop, 'code', flag='N')
                                name = self.xml_get_value_by_attr(shop, 'name', flag='N')
                                typepromo = self.xml_get_value_by_attr(shop, 'typepromo', flag='N')
                                if code == '0':
                                    code = None
                                if code:
                                    sql_text = 'execute procedure RBS_SHARE_IMPORT_FASTEN_OBJECTS(?,?,?,?,?,?)'
                                    sql_params = [None, WSETID, None, OBJID, code, typepromo]
                                    reswares = self.ExecuteSQL(sql_text,
                                                               sqlparams = sql_params,
                                                               fetch='none',
                                                               ExtVer=True)
                                    if reswares['status'] == krconst.kr_sql_error:
                                        self.LogFile(krconst.m_e_i_promo_fasten_obj % code, Terms=2)

    def ImportGroupMenegers(self, groupmenegers):
        """
            Импорт менеджеров для привязки к наборам
        """

        # получим спикок объектов на которых запущена система

        DepartmentIsInstall = self.GetDepartmentIsInstall()
        if DepartmentIsInstall:
            # получим коннект к Engine
            self.adbcode = None
            try:
                DBCODE = 'ENGINE'
            except:
                pass
            if DBCODE:
                adbcodecfg = self.read_config_other_db(DBCODE)
                if not adbcodecfg:
                    self.LogFile('Нет настроек доп БД!')
                    self.result['result'] = krconst.plugin_error
                    return False
                else:
                    self.adbcode = self.connect_other_db(adbcodecfg)
                    if not self.adbcode:
                        self.LogFile('Нет подключения к доп БД!')
                        self.result['result'] = krconst.plugin_error
                        return False
            for groupmeneger in groupmenegers:
                # получим группу
                groupgwarescode = self.xml_get_value_by_attr(groupmeneger, 'groupgwarescode')
                groupgwares = self.xml_get_value_by_attr(groupmeneger, 'groupgwares')

                gw = imp_wgroup.BaseWGroup()
                gw.parent_class = self
                gw.code = groupgwarescode
                gw.name = groupgwares
                gw.import_flag = 'S'
                gw.save()
                waresgrid = gw.wgroup_id

                if waresgrid:
                    # получим структуру менеджеров
                    managers = groupmeneger.find('managers')
                    if managers is not None:
                        for obj in managers:
                            codeuser = self.xml_get_value_by_attr(obj, 'code')
                            nameuser = self.xml_get_value_by_attr(obj, 'name')
                            departmentcode = self.xml_get_value_by_attr(obj, 'departmentcode')
                            departman = self.xml_get_value_by_attr(obj, 'department')
                            # проверим попадает ли пользователь с подразделением в список запущенных магазинов
                            if departmentcode in DepartmentIsInstall:
                                # получим manid пользователя, если он есть в БД
                                sqlparams = [codeuser, nameuser, None,'S', 'M', None, None, None, None, None, None, None, None, None, None, None]
                                manid = self.ImportCustomersSQL(sqlparams, codeuser, 'man', getObjid=True)
                                # если пользователь существует то продолжаем
                                if manid:
                                    # проверим привязано ли физ лицо к пользователям RBS
                                    sqlparams = [manid, nameuser, None, 'S', None]
                                    rbsuser = self.ImportGroupMenegersRBSSQL(sqlparams, nameuser)
                                    if rbsuser['status'] != krconst.kr_sql_error:
                                        # если id_user = None то пользователь еще не привязан или не существует
                                        # проверим есть ли он в ENGINE имени
                                        id_user = None
                                        if rbsuser['datalist']:
                                            id_user = rbsuser['datalist']['id_user']
                                        if not id_user:
                                            sqltext = '''select u.id_user, (select count(u1.id_user) as cntusers
                                                                              from engine_users u1
                                                                             where (UPPER(u1.FIO) = UPPER(?)) )
                                                           from ENGINE_USERS u
                                                          where (UPPER(u.FIO) = UPPER(?))'''
                                            engineuser = self.ExecuteSQL(sqltext,
                                                                         sqlparams=[nameuser, nameuser],
                                                                         fetch='one',
                                                                         db_local=self.adbcode,
                                                                         ExtVer=True)
                                            if engineuser['status'] == krconst.kr_sql_error:
                                                self.LogFile(krconst.m_e_importcheckuserengine % nameuser, Terms=2)
                                            else:
                                                # поверим сколько нашлось пользователь по ФИО
                                                # если больше одного то привязку делать не будем
                                                if engineuser['datalist']:
                                                    if engineuser['datalist']['cntusers'] > 1:
                                                        self.result['result'] = krconst.plugin_error
                                                        self.LogFile(krconst.m_e_importuserenginecnt, Terms=2)
                                                        id_user = None
                                                    else:
                                                        # если = 1 то добавлять не будем, просто привяжем пользвателя к фих лицу
                                                        if engineuser['datalist']['cntusers'] == 1:
                                                            id_user = engineuser['datalist']['id_user']
                                                            sqlparams = [manid, nameuser, id_user, 'I', None]
                                                            rbsuser = self.ImportGroupMenegersRBSSQL(sqlparams, nameuser)

                                                            if rbsuser['status'] == krconst.kr_sql_error:
                                                                id_user = None
                                                        # иначе добавим пользователя в Engine
                                                        else:
                                                            login = self.GenerateLogin(nameuser)
                                                            sqlparams = [None, None, login, nameuser, None, None, None, None]
                                                            id_user = self.ImportUserEngine(sqlparams, self.adbcode, nameuser)
                                                else:
                                                    login = self.GenerateLogin(nameuser)
                                                    sqlparams = [None, None, login, nameuser, None, None, None, None]
                                                    id_user = self.ImportUserEngine(sqlparams, self.adbcode, nameuser)

                                        if id_user:
                                            # привяжем к пользователю наборы
                                            res = self.ExecuteSQL('execute procedure RBS_Q_WARESGROUP_FASTEN_USER(?,?)',
                                                                  sqlparams = [id_user, waresgrid],
                                                                  fetch='none',
                                                                  ExtVer=True)
                                            if res['status'] == krconst.kr_sql_error:
                                                self.LogFile(krconst.kr_message_error_importfastenwguser % (groupgwares, nameuser), Terms=2)

                                        else:
                                            self.LogFile(krconst.kr_message_error_importusernull % nameuser, Terms=2)
                                            self.result['result'] = krconst.plugin_error
                #else:
                #    self.LogFile(krconst.kr_message_error_importwaresgroupnull % groupgwares, Terms=2)
                #    self.result['result'] = krconst.plugin_error

        else:
            self.LogFile(krconst.kr_message_warring_importgetinstallshop, Terms=2)


    def ImportGroupMenegersRBSSQL(self, paramssql, nameuser):
        """
            Проверка и привязка пользователя к физ лицу
        """

        res = self.ExecuteSQL('select * from RBS_Q_USER_INSSEL(?,?,?,?,?)',
                              sqlparams = paramssql,
                              fetch='one',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_importcheckuser % nameuser, Terms=2)
            rbsuser = {'status':res['status'], 'datalist':None}
        else:
            rbsuser = {'status':res['status'], 'datalist':res['datalist']}
        return rbsuser

    def GetDepartmentIsInstall(self):
        """
            Получим список объектов на которых запущена система
        """

        DepartmentIsInstall = []
        res = self.ExecuteSQL('select * from RBS_Q_INSTALL_LISTSHOPS',
                              sqlparams = [],
                              fetch='all',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_error_importgetinstallshop, Terms=2)
        else:
            # добавим пустую строку
            DepartmentIsInstall.append('')
            for dep in res['datalist']:
                DepartmentIsInstall.append(dep['OBJEXTERNALCODE'])
        return DepartmentIsInstall

    def ImportUserEngine(self, sqlparams, db_local, nameuser):
        """

        """
        id_user = None
        res = self.ExecuteSQL('select * from ENGINE_USER_ADD(?,?,?,?,?,?,?,?)',
                              sqlparams=sqlparams,
                              fetch='one',
                              db_local=db_local,
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_importadduserengine % nameuser, Terms=2)
        else:
            if res['datalist']:
                if not res['datalist']['ERROR_MSG']:
                    self.LogFile(res['datalist']['ERROR_MSG'], Terms=2)
                    self.result['result'] = krconst.plugin_error
                    id_user = None
                else:
                    id_user = res['datalist']['OUT_ID_USER']
            else:
                id_user = None
        return id_user

    def GenerateLogin(self, nameuser):
        re_search = re.search(r'([\S]+)\s+(.+)', nameuser)
        if re_search is None:
            F = nameuser
            I = ''
            O = ''
        else:
            F = re_search.group(1)
            IO = re_search.group(2)
            if F is None or IO is None:
                F = nameuser
                I = ''
                O = ''
            else:
                re_searchIO = re.search(r'([\S]+)\s+(.+)', IO)
                if re_searchIO is None:
                    I = IO
                    O = ''
                else:
                    I = re_searchIO.group(1)
                    O = re_searchIO.group(2)
                    if I is None or O is None:
                        I = IO
                        O = ''
        if len(I) > 0:
            I = I[0]
        if len(O) > 0:
            O = O[0]
        login = translit_to_ident(F, trunc_punctuation=True) + translit_to_ident(I, trunc_punctuation=True) + translit_to_ident(O, trunc_punctuation=True)
        return login

    def CheckGroupAsObj(self, codeobj):
        """
            проверка является ли группа объектом
        """

        res = self.ExecuteSQL('select * from rbs_q_config_import_obj r where r.nameobj = ? and r.groupasobj = ?',
                              sqlparams = ['DEPART', '1'],
                              fetch='one',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile('Bad' + krconst.kr_term_double_enter)

    def ImportDefViewUnit(self, warescode, externalid, secondaryname):
        """
            утановка параметра по умолчанию
        """

        res = self.ExecuteSQL('execute procedure RBS_Q_WARESUNIT_SETDEFVIEWUNIT(?,?)',
                              sqlparams = [warescode, externalid],
                              fetch='None',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_i_def_view_unit % warescode + krconst.kr_term_double_enter)

    def ImportWsets(self, wsets):
        """
            Импорт готовых наборов
        """

        for obj in wsets:
            # импортируем только ассортиметные наборы
            typewset = self.xml_get_value_by_attr(obj, 'typewset', flag='N')
            if typewset == 'A':
                codewset = self.xml_get_value_by_attr(obj, 'codewset', flag='N')
                namewset = self.xml_get_value_by_attr(obj, 'namewset', flag='N')
                docsubtype = self.xml_get_value_by_attr(obj, 'docsubtype', flag='N')
                docsubtypename = self.xml_get_value_by_attr(obj, 'docsubtypename', flag='N')
                paramssql = [codewset, namewset, typewset,  docsubtype, docsubtypename]
                resData = self.ImportWsetsSQL(paramssql, codewset, True)
                if resData:
                    # прязка наборов к объектам
                    objbondwsets = obj.find('objbondwsets')
                    for objbondwset in objbondwsets:
                        fromcode = self.xml_get_value_by_attr(objbondwset, 'fromcode', flag='N')
                        fromtype = self.xml_get_value_by_attr(objbondwset, 'fromtype', flag='N')
                        fromname = self.xml_get_value_by_attr(objbondwset, 'fromname', flag='N')
                        tocode = self.xml_get_value_by_attr(objbondwset, 'tocode', flag='N')
                        totype = self.xml_get_value_by_attr(objbondwset, 'totype', flag='N')
                        toname = self.xml_get_value_by_attr(objbondwset, 'toname', flag='N')
                        typeobjbond = self.xml_get_value_by_attr(objbondwset, 'typeobjbond', flag='N')
                        nameobjbond = self.xml_get_value_by_attr(objbondwset, 'nameobjbond', flag='N')
                        date1 = self.xml_get_value_by_attr(objbondwset, 'date1', flag='N')
                        date2 = self.xml_get_value_by_attr(objbondwset, 'date2', flag='N')
                        paramssql =[resData['WSETID'], fromcode, fromtype, fromname, tocode, totype, toname, typeobjbond, date1, date2]
                        self.ImportWsetObjbondSQL(paramssql, codewset)

                    # состав набора
                    waresinsets = obj.find('waresinsets')
                    if waresinsets is not None:
                        for waresinset in waresinsets:
                            warescode = self.xml_get_value_by_attr(waresinset, 'warescode', flag='N')
                            namewares = self.xml_get_value_by_attr(waresinset, 'namewares', flag='N')
                            mpp = self.xml_get_value_by_attr(waresinset, 'mpp', flag='N')
                            mpo = self.xml_get_value_by_attr(waresinset, 'mpo', flag='N')
                            codeunit = self.xml_get_value_by_attr(waresinset, 'codeunit', flag='N')
                            price = self.xml_get_value_by_attr(waresinset, 'price', flag='N')
                            date1 = self.xml_get_value_by_attr(waresinset, 'date1', flag='N')
                            date2 = self.xml_get_value_by_attr(waresinset, 'date2', flag='N')
                            paramssql = [resData['WSETID'], warescode, namewares, mpp, mpo, codeunit, price, date1, date2, resData['DOCSUBTYPEID']]
                            self.ImportWinsetsSQL(paramssql, codewset, warescode)

    def ImportWsetsSQL(self, paramssql, codewset, getID=False):
        """
            Импорт готовых наборов SQL
        """

        resData = {}
        res = self.ExecuteSQL('select * from RBS_Q_WSET_IMP(?,?,?,?,?)',
                              sqlparams = paramssql,
                              fetch='one',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_error_importwset + ' ' + codewset + krconst.kr_term_double_enter)
        else:
            if getID:
                resData['WSETID'] = res['datalist']['WSETID']
                resData['DOCSUBTYPEID'] = res['datalist']['DOCSUBTYPEID']
        return resData

    def ImportWsetObjbondSQL(self, paramssql, codewset):
        """
            Импорт привязок наборов
        """

        res = self.ExecuteSQL('execute procedure RBS_Q_WSETOBJBOND_IMP(?,?,?,?,?,?,?,?,?,?)',
                              sqlparams = paramssql,
                              fetch='none',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_error_importwsetobjbond + ' ' + codewset + krconst.kr_term_double_enter)


    def ImportWinsetsSQL(self, paramssql, codewset, warescode):
        """
            Импорт готовых наборов SQL - товары
        """

        res = self.ExecuteSQL('execute procedure RBS_Q_WINSET_IMP(?,?,?,?,?,?,?,?,?,?)',
                              sqlparams = paramssql,
                              fetch='none',
                              ExtVer=True)
        if res['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.kr_message_error_importwset + ' ' + codewset)
            self.LogFile(krconst.m_e_i_wset_one % warescode + krconst.kr_term_double_enter)

    def import_price_list(self, pricelists):
        """
            Импорт прайс листов
        """

        for pricelist in pricelists:
            date = self.xml_get_value_by_attr(pricelist, 'date', flag='N')
            format = self.xml_get_value_by_attr(pricelist, 'format', flag='N')
            customer = self.xml_get_value_by_attr(pricelist, 'customer', flag='N')
            customercode = self.xml_get_value_by_attr(pricelist, 'customercode', flag='N')
            waresname = self.xml_get_value_by_attr(pricelist, 'wares', flag='N')
            warescode = self.xml_get_value_by_attr(pricelist, 'warescode', flag='N')
            price = self.xml_get_value_by_attr(pricelist, 'price', flag='N')
            if customercode and customercode:

                sql_text = 'execute procedure RBS_Q_BUYPRICE_IMPORT(?,?,?,?,?,?,?)'
                sql_params = [customercode, customer, warescode, waresname, date, price, format]

                res = self.ExecuteSQL(sql_text,
                                      sqlparams = sql_params,
                                      fetch='none',
                                      ExtVer=True)
                if res['status'] == krconst.kr_sql_error:
                    self.LogFile(krconst.m_e_i_wares_price_list % warescode + krconst.kr_term_double_enter)

    def import_gwares_units(self, gwares):
        """
            Импорт ед измерений товара
        """

        for obj in gwares:
            external_id = None
            wares_code = self.xml_get_value_by_attr(obj, 'code', flag='N')
            if self.xml_name_external_id:
                external_id = self.xml_get_value_by_attr(obj, self.xml_name_external_id, flag='N')

            # находим ШК
            barcodes = obj.find('barcodes')
            if barcodes is not None:
                self.import_wares_barcode(wares_code, None, barcodes, external_id)

    def import_contracts(self, contracts):
        """
            Импорт договоров
        """

        sql_text = 'execute procedure RBS_Q_CONTACTS_PREIMPORT'
        resupdate = self.execute_sql(sql_text,
                                     sql_params = [],
                                     fetch='none')
        if resupdate['status'] == krconst.kr_sql_error:
            self.LogFile(krconst.m_e_i_contracts_update_before + krconst.t_double_enter)

        for contract in contracts:
            externalcode = self.xml_get_value_by_attr(contract, 'id1c', flag='N')
            contracttype = self.xml_get_value_by_attr(contract, 'contracttype', flag='N')
            customername = self.xml_get_value_by_attr(contract, 'customername', flag='N')
            customercode = self.xml_get_value_by_attr(contract, 'customercode', flag='N')
            code = self.xml_get_value_by_attr(contract, 'code', flag='N')
            name = self.xml_get_value_by_attr(contract, 'name', flag='N')
            ismain = str_to_bool_int(self.xml_get_value_by_attr(contract, 'maincontract', flag='N'))
            dateuse = self.xml_get_value_by_attr(contract, 'dateuse', flag='N')
            deletemarker = str_to_bool_int(self.xml_get_value_by_attr(contract, 'deletemarker', flag='N'))
            # тут используется как status нужно инвертировать
            if deletemarker == '1':
                deletemarker = '0'
            else:
                deletemarker = '1'

            if dateuse == '01.01.0001 0:00:00':
                dateuse = ''

            sql_text = 'select * from RBS_Q_CONTACTS_INSSEL(?,?,?,?,?,?,?,?,?)'
            sql_params = [externalcode, contracttype, customername, customercode,
                          code, name, dateuse, ismain, deletemarker]
            res = self.execute_sql(sql_text,
                                   sql_params = sql_params,
                                   fetch='many')
            if res['status'] == krconst.kr_sql_error:
                self.LogFile(krconst.m_e_i_contracts % externalcode + krconst.t_double_enter)
            else:
                makers = contract.find('makers')
                for itm in res['datalist']:
                    flag_error = False
                    if makers is not None:
                        for maker in makers:
                            name_maker = self.xml_get_value_by_attr(maker, 'name', flag='N')
                            code_maker = self.xml_get_value_by_attr(maker, 'code', flag='N')

                            sql_text = 'execute procedure RBS_Q_CONTACTS_MAKERS_INSSEL(?,?,?)'
                            sql_params = [itm['CONTRACTID'], code_maker, name_maker]
                            resm = self.execute_sql(sql_text,
                                                    sql_params = sql_params,
                                                    fetch='none')
                            if resm['status'] == krconst.kr_sql_error:
                                self.LogFile(krconst.m_e_i_contracts_maker % externalcode + krconst.t_double_enter)
                                flag_error = True
                    if flag_error:
                        self.LogFile('Удаление лишних производителей не может быть произведено.' + krconst.t_double_enter)
                    else:
                        sql_text = 'execute procedure RBS_Q_CONTACTS_AFTERIMPORT(?)'
                        res_update = self.execute_sql(sql_text,
                                                      sql_params = [itm['CONTRACTID']],
                                                      fetch='none')
                        if res_update['status'] == krconst.kr_sql_error:
                            self.LogFile(krconst.m_e_i_contracts_update_after + krconst.t_double_enter)
