# -*- coding: utf-8 -*-
"""
    swat 14.01.2014
    version 0.0.2.0
    модуль импорта товаров
"""

from operator import itemgetter

import krconst as k
from rbsqutils import str_to_bool_int
from rbsqutils import BarcodeToDic
from rbsqutils import unit_to_list


class Gwares():
    """
        Класс  импорта товаров
    """

    _parent_class = None

    _name = None
    _code = None
    _main_unit = None
    _second_unit = None
    _parent_code = None
    _articul = None
    _tax = None
    _delete_marker = None
    _expiration_type = None
    _expiration_value = None
    _parent = None
    _check_w_group = None
    _check_w_group_value = None
    _external_id = None
    _group_id = None
    _factor = None
    _mpp = None
    _sezon = None
    _characteristic = []

    __obj = None
    __type_import = None

    is_import_wares = True
    waresid = None

    def __init__(self, parent_class, obj=None):
        """
            Инициализация переменных из XML
        """

        self._characteristic = []

        self._parent_class = parent_class
        self.__obj = obj

        ''' сделаем проверку на ассортимент склада, если включен фильтр '''
        if parent_class.gwares_check_assortment == '1':
            suppliers = obj.find('suppliers')
            if suppliers is not None:
                self.is_import_wares = self.__gwares_check_assortment_obj(suppliers)
            else:
                self.is_import_wares = False

        if self.is_import_wares:
            if obj is not None:
                self._code = self._xml_get_value(obj, 'warescode', flag='N')
                self._name = self._xml_get_value(obj, 'waresname', flag='N')
                self._delete_marker = self._xml_get_value(obj, parent_class.xml_name_delete_flag, flag='N')
                self._delete_marker = str_to_bool_int(self._delete_marker)
                self._parent = self._xml_get_value(obj, 'parent', flag='N')
                self._parent_code = self._xml_get_value(obj, 'parentcode', flag='N')
                if parent_class.xml_name_external_id:
                    self._external_id = self._xml_get_value(obj, parent_class.xml_name_external_id, flag='N')
                    self._group_id = self._xml_get_value(obj, parent_class.xml_name_external_id + 'parent', flag='N')
                    if not self._group_id:
                        self._group_id = self._xml_get_value(obj, 'parent' + parent_class.xml_name_external_id, flag='N')


                if self._parent_code == '0':
                    self._parent_code = None

                ''' для поддержки старых форматов '''
                if not self._code:
                    self._code = self._xml_get_value(obj, 'code', flag='N')
                if not self._name:
                    self._name = self._xml_get_value(obj, 'name', flag='N')

                self._articul = self._xml_get_value(obj, 'articul', flag='N')
                self._tax = self._xml_get_value(obj, 'tax', flag='N')
                self._main_unit = self._xml_get_value(obj, 'mainunit', flag='N')
                self._second_unit = self._xml_get_value(obj, 'secondunit', flag='N')
                self._factor = self._xml_get_value(obj, 'factor', flag='N')
                self._mpp = self._xml_get_value(obj, 'mpp', flag='N')
                self._sezon = self._xml_get_value(obj, 'sezon', flag='N')

                if self._second_unit == '0':
                    self._second_unit = None

                self._expiration_value = self._xml_get_value(obj, 'expirationvalue', flag='N')
                self._expiration_type = self._xml_get_value(obj, 'expirationtype', flag='N')

                if self._expiration_type == "День":
                    self._expiration_type = 'D'
                if self._expiration_type == "Срок хранения (дней)":
                    self._expiration_type = 'D'
                if self._expiration_type == "Месяц":
                    self._expiration_type = 'M'
                if self._expiration_type == "Час":
                    self._expiration_type = 'h'
                if self._expiration_type == "Неделя":
                    self._expiration_type = 'W'
                if self._expiration_type == "Год":
                    self._expiration_type = 'Y'

                ''' Характеристику товара '''
                self._get_characteristic()

    def _get_characteristic(self):
        """
            Получение характеристики товара
        """
        if self.__obj is not None:
            characteristics = self.__obj.find('characteristics')
            if characteristics is not None:
                for itm in characteristics:
                    name = self._xml_get_value(itm, 'name', flag='N')
                    delete_marker = self._xml_get_value(itm, self._parent_class.xml_name_delete_flag, flag='N')
                    delete_marker = str_to_bool_int(delete_marker)
                    id_char = self._xml_get_value(itm, 'id', flag='N')
                    self._characteristic.append({'name': name,
                                                 'delete_marker': delete_marker,
                                                 'id': id_char})

    def save(self):
        """
            сохраним товар

            Существует два варианта структуры данных
                    1. Основная ед измерения указана в теге <wares>,
                    и нет блока с ед. измерения
                    2. Ед. измерения вынесены в отдельные теги
        """

        if self.__obj.get('mainunit'):
            self.__type_import = 1
        else:
            self.__type_import = 2
            self._main_unit = None

        ''' Если есть характеристики товара, то обрабатываем в цикле '''
        sql_text = 'select * from RBS_Q_GWARES_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

        if len(self._characteristic) == 0:
            self._characteristic.append({'name': None,
                                         'delete_marker': '0',
                                         'id': None})
        else:
            if self._parent_class.add_none_characteristic == '1':
                self._characteristic.append({'name': None,
                                             'delete_marker': '0',
                                             'id': None})

        for secondary_name in sorted(self._characteristic, key=lambda x: x['id']):
            """ Сделаем проверку на признак удаления """
            delete_marker = self._delete_marker
            if secondary_name['name']:
                if self._delete_marker != secondary_name['delete_marker']:
                    delete_marker = '1'

            sql_params = [self._name, self._code, self._main_unit, self._parent_code, self._articul,
                          self._tax, delete_marker, 'I', self._expiration_type,
                          self._expiration_value, self._parent, self._check_w_group,
                          self._check_w_group_value, self._external_id, secondary_name['name'], self._group_id,
                          self._parent_class.gwares_ubd, secondary_name['id']]

            res = self._parent_class.ExecuteSQL(sql_text,
                                                sqlparams=sql_params,
                                                fetch='one',
                                                ExtVer=True)
            if res['status'] == k.kr_sql_error:
                self._parent_class.LogFile(k.m_e_i_wares % self._code + k.kr_term_double_enter)
            else:
                self.waresid = res['datalist']['waresid']
                if self.waresid is not None:
                    if self.__type_import == 1:
                        ''' добавим вторичную ед измерения
                            если она существует и удовлетворяет требованиям.
                            Сдесь дополнительная
                            характеристика товара не участвует
                        '''
                        if self._second_unit \
                                and (self._second_unit != self._main_unit) \
                                and ('Объект не найден' not in self._second_unit) \
                                and self._factor:
                            sql_params = [self._code, self._second_unit, self._factor, 'mainpack',
                                          None, None, None, None, None, self._parent_class.coef_ttx,
                                          None, self._external_id, None, None]
                            self.__save_wares_unit(sql_params, self._second_unit)

                        ''' ШК '''
                        self.barcode(self.__obj.find('barcodes'), self._parent_class.create_ttx)

                        ''' проверям есть ли производитель товара '''
                        self.__save_wares_maker()

                        ''' проверям есть ли история налоговых ставок для продажи '''
                        self.__save_wares_tax_sale()

                        ''' проверяем есть ли привязка товаров к поставщикам
                            и генерируем ассортимент при необходимости
                        '''
                        self.__save_assortment()

                        ''' проставляем активность для РЦ, при необходимости '''
                        self.__save_wares_wh_status()

                        ''' импорт типа товара '''
                        self.__save_wares_type()

                        ''' импорт МПО '''
                        self.__save_wares_mpo()

                        ''' импорт сезонности товара '''
                        self.__save_wares_season()

                        ''' установка ед отображения '''
                        self.set_def_view_unit()

                        # Нарезка для товаров. Данный метод нужен для АЗ Невады
                        self._set_slice()

                        # тип алкогольной продукции
                        self.__set_alco()

        # if self.waresid is not None:
        if self.__type_import == 2 and self._delete_marker != '1':
            ''' забираем доп атрибуты товара + ШК '''
            wares_units = self.__obj.find('waresunits')
            if wares_units is not None:
                ''' находим основную ед измерения '''
                self._wares_unit(wares_units, 'mainunit')
                ''' находим упаковку '''
                self._wares_unit(wares_units, 'mainpack')
                ''' находим паллету '''
                self._wares_unit(wares_units, 'mainpallet')
                ''' находим не основную упаковку '''
                self._wares_unit(wares_units, 'pack')
                ''' находим ед для отчетов '''
                self._wares_unit(wares_units, 'report')
                ''' находим все остальное '''
                self._wares_unit(wares_units, '')

    def barcode(self, barcodes, create_ttx, unit=None, unit_uuid=None):
        """
            ШК
        """

        wares_barcode = []
        uweight = None
        ulength = None
        uheight = None
        uwidth = None
        get_units = 1
        if unit or unit_uuid:
            get_units = 0

        #barcodes = self.__obj.find('barcodes')
        if barcodes is not None:
            for barcode in barcodes:
                wu_delete_marker = self._xml_get_value(barcode, self._parent_class.xml_name_delete_flag, flag='N')
                wu_delete_marker = str_to_bool_int(wu_delete_marker)
                if wu_delete_marker == 0 or wu_delete_marker == '0' or wu_delete_marker is None:
                    factor = self._xml_get_value(barcode, 'factor', flag='N')
                    if get_units == 1:
                        unit = self._xml_get_value(barcode, 'unit')
                    if create_ttx == '1':
                        uweight = self._xml_get_value(barcode, 'uweight', flag='N')
                        ulength = self._xml_get_value(barcode, 'ulength', flag='N')
                        uheight = self._xml_get_value(barcode, 'uheight', flag='N')
                        uwidth = self._xml_get_value(barcode, 'uwidth', flag='N')
                    characteristic = self._xml_get_value(barcode, 'characteristic', flag='N')
                    characteristic_id = self._xml_get_value(barcode, 'characteristic_id', flag='N')
                    if characteristic_id == '00000000-0000-0000-0000-000000000000':
                        characteristic_id = None
                    wares_barcode = BarcodeToDic(wares_barcode, self._xml_get_value(barcode, 'value'), unit if unit else unit_uuid, factor,
                                                 uweight, ulength, uheight, uwidth, characteristic, characteristic_id)
            if wares_barcode:
                ''' Отсортируем список, что бы вначеле шли ед с factor = 1 '''
                wares_barcode = sorted(wares_barcode, key=itemgetter('factor'))

                if len(self._characteristic) == 0:
                    ''' Импорт ШК без наличия характеристик '''
                    for itm in wares_barcode:
                        sql_params = [self._code, itm['unit'] if unit else None, itm['barcode'], itm['factor'],
                                      itm['uweight'], itm['ulength'], itm['uheight'], itm['uwidth'],
                                      self._parent_class.coef_ttx, None, self._external_id,
                                      None, itm['unit'] if not unit else None]
                        self._save_barcode(sql_params, itm['barcode'])
                else:
                    ''' Импорт ШК с характеристиками '''
                    for itm in wares_barcode:
                        if itm['characteristic'] is not None or itm['characteristic_id'] is not None:
                            sql_params = [self._code, itm['unit'] if unit else None, itm['barcode'], itm['factor'],
                                          itm['uweight'], itm['ulength'], itm['uheight'], itm['uwidth'],
                                          self._parent_class.coef_ttx, itm['characteristic'], self._external_id,
                                          itm['idcharacteristic'], itm['unit'] if not unit else None]
                            self._save_barcode(sql_params, itm['barcode'])

                    ''' Получим список ед измерений '''
                    unit_list = []
                    for itm in wares_barcode:
                        unit_list = unit_to_list(unit_list, itm['unit'])

                    ''' Делаем полный проход по характеристикам и Ед измерения '''
                    for char_name in self._characteristic:
                        if char_name['delete_marker'] == '0':
                            for unit_name in unit_list:
                                for itm in wares_barcode:
                                    if (itm['characteristic'] is None or itm['characteristic_id'] is None)and itm['unit'] == unit_name:
                                        sql_params = [self._code, itm['unit'] if unit else None, itm['barcode'], itm['factor'],
                                                      itm['uweight'], itm['ulength'], itm['uheight'], itm['uwidth'],
                                                      self._parent_class.coef_ttx, char_name['name'], self._external_id,
                                                      char_name['id'], itm['unit'] if not unit else None]
                                        self._save_barcode(sql_params, itm['barcode'])

    def _save_barcode(self, sql_params, barcode):
        """
            Сохранение ШК
        """

        sql_text = 'select * from RBS_Q_IMP_WARESBARCODE(?,?,?,?,?,?,?,?,?,?,?,?,?)'
        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams=sql_params,
                                            fetch='one',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_wares % self._code + '. ' + k.m_e_i_wares_barcode % barcode
            self._parent_class.LogFile(message + k.kr_term_double_enter)

    def _set_slice(self):
        """
            Сохранение нарезки. Для АЗ Невада
        """
        if self.__obj is not None:
            slices = self.__obj.find('sliced')
            if slices is not None:
                for itm in slices:
                    slicecode = self._xml_get_value(itm, 'slicecode', flag='N')
                    status = self._xml_get_value(itm, 'active', flag='N')
                    status = str_to_bool_int(status)

                    sql_text = 'execute procedure Q_RECIPE_SLICE_INS(?,?,?)'
                    sql_params = [self.waresid, slicecode, status]
                    res = self._parent_class.execute_sql(sql_text,
                                                         sql_params=sql_params,
                                                         fetch='none')
                    if res['status'] == k.kr_sql_error:
                        message = 'Ошибка сохранения разреза товара %s' % self._code
                        self._parent_class.log_file(message, terms=1)

    def _wares_unit(self, wares_units, type_wares_unit):
        """
            ед измерения
        """

        for obj in wares_units:
            if self._xml_get_value(obj, 'type') == type_wares_unit:
                unit_uuid = None
                unit = self._xml_get_value(obj, 'unit', flag='N')
                if not unit:
                    unit_uuid = self._xml_get_value(obj, 'uuid', flag='N')
                if unit or unit_uuid:
                    factor = self._xml_get_value(obj, 'factor', flag='N')
                    ulength = None
                    uwidth = None
                    uheight = None
                    uweight = None
                    unetweight = None
                    if self._parent_class.create_ttx == '1':
                        ulength = self._xml_get_value(obj, 'ulength', flag='N')
                        uwidth = self._xml_get_value(obj, 'uwidth', flag='N')
                        uheight = self._xml_get_value(obj, 'uheight', flag='N')
                        uweight = self._xml_get_value(obj, 'uweight', flag='N')
                        unetweight = self._xml_get_value(obj, 'unetweight', flag='N')

                    if len(self._characteristic) == 0:
                        sql_params = [self._code, unit, factor, type_wares_unit, ulength, uwidth, uheight,
                                      uweight, unetweight, self._parent_class.coef_ttx, None, self._external_id, None, unit_uuid]
                        if self.__save_wares_unit(sql_params, unit):
                            ''' находим ШК '''
                            barcodes = obj.find('barcodes')
                            if barcodes is not None:
                                self.barcode(barcodes, '0', unit)
                    else:
                        for char_name in self._characteristic:
                            if char_name['delete_marker'] == '0':
                                sql_params = [self._code, unit, factor, type_wares_unit, ulength, uwidth, uheight,
                                              uweight, unetweight, self._parent_class.coef_ttx, char_name['name'], self._external_id,
                                              char_name['id'], unit_uuid]
                                self.__save_wares_unit(sql_params, unit if unit else unit_uuid)
                        ''' находим ШК '''
                        barcodes = obj.find('barcodes')
                        if barcodes is not None:
                            self.barcode(barcodes, '0', unit, unit_uuid)

    def __save_wares_unit(self, sql_params, name_unit):
        """
            Сохранение ед измерения
        """

        result = True

        sql_text = 'select * from RBS_Q_WARESUNIT_INSSEL(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams=sql_params,
                                            fetch='one',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            message = k.m_e_i_wares % self._code + '. ' + k.m_e_i_wares_unit % name_unit
            self._parent_class.LogFile(message + k.kr_term_double_enter)
            result = False

        return result

    def __save_wares_maker(self):
        """
            Импорт производителей товара
        """

        maker = self._xml_get_value(self.__obj, 'maker', flag='N')
        maker_code = self._xml_get_value(self.__obj, 'makercode', flag='N')

        if maker_code and maker_code != '0':

            sql_text = 'execute procedure RBS_Q_GWARESMAKER_INS(?,?,?)'
            sql_params = [self._code, maker_code, maker]

            res = self._parent_class.ExecuteSQL(sql_text,
                                                sqlparams=sql_params,
                                                fetch='none',
                                                ExtVer=True)
            if res['status'] == k.kr_sql_error:
                self._parent_class.LogFile(k.m_e_i_wares_maker % self._code, Terms=2)

    def __save_wares_tax_sale(self):
        """
            Импорт налоговых ставок
        """

        nds_wares = self.__obj.find(self._parent_class.xml_tax_name_tag)
        if nds_wares is not None:
            if nds_wares is not None:
                for nds in nds_wares:
                    tax_code = self._xml_get_value(nds, 'value', flag='N')
                    date_start = self._xml_get_value(nds, 'date', flag='N')

                    if tax_code and tax_code != '0':

                        sql_text = 'execute procedure RBS_Q_WARESTAX_INSSEL(?,?,?)'
                        sql_params = [self._code, tax_code, date_start]

                        res = self._parent_class.ExecuteSQL(sql_text,
                                                            sqlparams=sql_params,
                                                            fetch='none',
                                                            ExtVer=True)
                        if res['status'] == k.kr_sql_error:
                            self._parent_class.LogFile(k.m_e_i_wares_tax % self._code, Terms=2)

    def __save_assortment(self):
        """
            Импорт поставщиков + генерация ассортимента + наборы
        """

        if self._parent_class.gwares_create_assortment == '1':
            suppliers = self.__obj.find('suppliers')

            if suppliers is not None:
                for obj in suppliers:
                    supplier_code = self._xml_get_value(obj, 'code', flag='N')
                    supplier_name = self._xml_get_value(obj, 'name', flag='N')
                    order_restrict = self._xml_get_value(obj, 'noactivityvalue', flag='N')
                    order_restrict = str_to_bool_int(order_restrict)
                    no_activity_date = self._xml_get_value(obj, 'noactivitydate', flag='N')
                    format_obj = self._xml_get_value(obj, 'format', flag='N')

                    if not order_restrict:
                        order_restrict = '0'

                    sql_text = 'execute procedure RBS_Q_GWARESSUPPLIERS_INSSEL(?,?,?,?,?,?,?)'
                    sql_params = [self._code, supplier_code, supplier_name,
                                  order_restrict, self._mpp, format_obj, no_activity_date]

                    res = self._parent_class.ExecuteSQL(sql_text,
                                                        sqlparams=sql_params,
                                                        fetch='none',
                                                        ExtVer=True)
                    if res['status'] == k.kr_sql_error:
                        self._parent_class.LogFile(k.m_e_i_wares_suppliers % self._code, Terms=2)

                ''' обработка товара для корректировки наборов '''
                if self._parent_class.gwares_create_wset == '1':

                    sql_text = 'execute procedure RBS_Q_WARESSET_GENERATE(?)'
                    sql_params = [self._code]

                    res = self._parent_class.ExecuteSQL(sql_text,
                                                        sqlparams=sql_params,
                                                        fetch='none',
                                                        ExtVer=True)
                    if res['status'] == k.kr_sql_error:
                        self._parent_class.LogFile(k.m_e_i_wset_one % self._code, Terms=2)

            # импортируем ассортимент склада
            assort_rc = self.__obj.find('assort_rc')
            if assort_rc is not None:
                if self._xml_get_value(assort_rc, 'value', flag='N') == 'Да':
                    for obj in assort_rc:
                        format_obj = self._xml_get_value(obj, 'format')
                        if format_obj in ('РЦ', ''):
                            supplier_code = self._xml_get_value(obj, 'code', flag='N')
                            supplier_name = self._xml_get_value(obj, 'name', flag='N')
                            order_restrict = self._xml_get_value(obj, 'noactivityvalue', flag='N')
                            order_restrict = str_to_bool_int(order_restrict)
                            no_activity_date = self._xml_get_value(obj, 'noactivitydate', flag='N')

                            if not order_restrict:
                                order_restrict = '0'

                            sql_text = 'execute procedure RBS_Q_GW_SUPPLIERS_SWH_INSSEL(?,?,?,?,?,?,?)'
                            sql_params = [self._code, supplier_code, supplier_name,
                                          order_restrict, self._mpp, format_obj, no_activity_date]

                            res = self._parent_class.ExecuteSQL(sql_text,
                                                                sqlparams=sql_params,
                                                                fetch='none',
                                                                ExtVer=True)
                            if res['status'] == k.kr_sql_error:
                                self._parent_class.LogFile(k.m_e_i_wares_suppliers % self._code, Terms=2)

    def __save_wares_wh_status(self):
        """
            Заполняем активность для скалада
        """

        if self._parent_class.gwares_check_assortment == '1':
            return False
        suppliers = self.__obj.find('suppliers')
        if suppliers is not None:
            for obj in suppliers:
                code = self._xml_get_value(obj, 'code')
                if ',' + code + ',' in self._parent_class.gwares_check_assortment_filter:
                    format_obj = self._xml_get_value(obj, 'format', flag='N')
                    no_activity_value = self._xml_get_value(obj, 'noactivityvalue', flag='N')
                    no_activity_value = str_to_bool_int(no_activity_value)

                    if no_activity_value == '0':
                        no_activity_value = '1'
                    else:
                        if no_activity_value == '1':
                            no_activity_value = '0'

                    sql_text = 'execute procedure RBS_Q_WARES_OPTIONS(?,?,?)'
                    sql_params = [self._code, format_obj, no_activity_value]

                    res = self._parent_class.ExecuteSQL(sql_text,
                                                        sqlparams=sql_params,
                                                        fetch='none',
                                                        ExtVer=True)
                    if res['status'] == k.kr_sql_error:
                        message = k.m_e_i_format_wares % (format_obj, self._code) + k.kr_term_double_enter
                        self._parent_class.LogFile(message)

    def set_def_view_unit(self):
        """
            утановка параметра по умолчанию
        """

        sql_text = 'execute procedure RBS_Q_WARESUNIT_SETDEFVIEWUNIT(?,?)'
        sql_params = [self._code, self._external_id]
        res = self._parent_class.ExecuteSQL(sql_text,
                                            sqlparams=sql_params,
                                            fetch='None',
                                            ExtVer=True)
        if res['status'] == k.kr_sql_error:
            self._parent_class.LogFile(k.m_e_i_def_view_unit % self._code + k.kr_term_double_enter)

    def __save_wares_type(self):
        """
            Импорт типов товара для ассортиментной матрицы
        """

        if self._parent_class.gwares_create_type == '1':
            statuses = self.__obj.find('statuses')
            if statuses is not None:
                for obj in statuses:
                    status = self._xml_get_value(obj, 'status', flag='N')
                    if status:
                        format_obj = self._xml_get_value(obj, 'format')

                        sql_text = 'execute procedure RBS_Q_GWARESSTATUSASS_INSSEL(?,?,?)'
                        sql_params = [self._code, status, format_obj]

                        res = self._parent_class.ExecuteSQL(sql_text,
                                                            sqlparams=sql_params,
                                                            fetch='all',
                                                            ExtVer=True)
                        if res['status'] == k.kr_sql_error:
                            self._parent_class.LogFile(k.m_e_i_wares_type % self._code, Terms=2)

    def __save_wares_mpo(self):
        """
            Импорт МПО
        """

        if self._parent_class.gwares_create_mpo == '1':
            mpos = self.__obj.find('mpos')
            if mpos is not None:
                if mpos is not None:
                    for obj in mpos:
                        departmen_code = self._xml_get_value(obj, 'departmencode', flag='N')
                        departmen = self._xml_get_value(obj, 'departmen', flag='N')
                        mpo = self._xml_get_value(obj, 'value', flag='N')
                        if departmen_code != '0':

                            sql_text = 'execute procedure RBS_Q_GWARES_MPO_SET(?,?,?,?)'
                            sql_params = [self._code, departmen_code, departmen, mpo]

                            res = self._parent_class.ExecuteSQL(sql_text,
                                                                sqlparams=sql_params,
                                                                fetch='all',
                                                                ExtVer=True)
                            if res['status'] == k.kr_sql_error:
                                message = k.m_e_i_wares_mpo % self._code
                                self._parent_class.LogFile(message, Terms=2)

    def __save_wares_season(self):
        """
            Импорт сезонности товара
        """

        if self._parent_class.gwares_create_season == '1':
            if self._sezon:

                sql_text = 'execute procedure RBS_Q_WARES_SEASON_SET(?,?)'
                sql_params = [self._code, self._sezon]

                res = self._parent_class.ExecuteSQL(sql_text,
                                                    sqlparams=sql_params,
                                                    fetch='none',
                                                    ExtVer=True)
                if res['status'] == k.kr_sql_error:
                    message = k.m_e_i_wares_season % self._code + k.kr_term_double_enter
                    self._parent_class.LogFile(message)

    def __gwares_check_assortment_obj(self, suppliers):
        """
            Проверка является ли поставщик товара РЦ
        """

        if self._parent_class.gwares_check_assortment_filter == '':
            return False
        for obj in suppliers:
            code = self._xml_get_value(obj, 'code')
            if ',' + code + ',' in self._parent_class.gwares_check_assortment_filter:
                return True
        return False

    def __set_alco(self):
        """
        Сохранение типа аолкогольной продукции
        @return:
        """

        import_alco = self._xml_get_value(self.__obj, 'importalco', flag='N')
        if import_alco is not None:
            import_alco = str_to_bool_int(import_alco)
            alco_type_code = self._xml_get_value(self.__obj, 'alcotypecode', flag='N')
            alco_type_name = self._xml_get_value(self.__obj, 'alcotype', flag='N')
            # сделано так, потому что еще не выложено на сервер
            multi_alco = None
            try:
                multi_alco = self._xml_get_value(self.__obj, 'multialco', flag='N')
                multi_alco = str_to_bool_int(multi_alco)
            except:
                pass
            sql_text = 'execute procedure RBS_Q_GWARES_ALCOTYPES_SET(?,?,?,?,?)'
            sql_params = [self.waresid, alco_type_code, alco_type_name, import_alco, multi_alco]

            res = self._parent_class.execute_sql(sql_text,
                                                 sql_params=sql_params,
                                                 fetch='none')
            if res['status'] == k.kr_sql_error:
                message = 'Ошибка импорта признака алкогольной продукции:' + self._code
                self._parent_class.log_file(message, terms=1)

    def _xml_get_value(self, xml, attr, flag='E'):
        """
            Для короткого вызова
        """

        return self._parent_class.xml_get_value_by_attr(xml, attr, flag)