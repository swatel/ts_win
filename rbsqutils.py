# -*- coding: cp1251-*

import re
import os
import time
import sys
import glob
import traceback

from mx.DateTime import now
from datetime import timedelta

from xml.dom import minidom

import krconst

' число в любом виде '
Regex_Number = re.compile(r"(?i)^(\+|-)?[0-9]*\.?[0-9]*((?<=([0-9]|\.))e(\+|-)?[0-9]+)?$")

Zip_Errors = {
    0: '',
    1: 'Warning (Non fatal error(s)). '
       'For example, one or more files were locked by some other application, '
       'so they were not compressed.',
    2: 'Fatal error',
    7: 'Command line error',
    8: 'Not enough memory for operation',
    255: 'User stopped the process',
    256: 'Can not find file'
}


def formatMxDateTime(mxDateTime, format=None, sformatFrom='%Y-%m-%d %H:%M:%S'):
    if mxDateTime is None:
        return None
    sdate = str(mxDateTime).split('.')[0]
    sdateTuple = time.strptime(sdate, sformatFrom)
    if sdateTuple[5] == 60:
        sdateTuple = list(sdateTuple)
        sdateTuple[5] = 59
        sdateTuple = tuple(sdateTuple)
    return time.strftime(format, sdateTuple)


def decodeXStr(text):
    """
        Перекодирует строку
    """

    text = str(text)
    letter_list = text.split('\\x')
    ret = ''
    first = True
    for letter_code in letter_list:
        if not first:
            try:
                code = int(letter_code[:2], 16)
                ret += chr(code) + letter_code[2:] #срабатывает в случае последнего символа
            except:
                ret += '\\x%s' % letter_code
        else:
            #то, что до первого \x
            first = False
            ret += '%s' % letter_code
    return ret


def decodeUStr(s):
    r"""Преобразует последовательности, состоящие из символов юникода вида \uNNNN
     в однобайтовые символы в кодировке cp1251 (иногда требуется после json.dumps)

    """
    s = str(s)
    letter_list = s.split('\\u')
    ret = ''
    first = True
    for letter_code in letter_list:
        if not first:
            try:
                code = int(letter_code[:4], 16)
                ret += unichr(code).encode('cp1251') + letter_code[4:] #срабатывает в случае последнего символа
            except:
                ret += '\\u%s' % letter_code
        else:
            #то, что до первого \x
            first = False
            ret += '%s' % letter_code
    return ret


def empty_str_to_null(val):
    """
        возвращает None, если строка пустая
    """

    if val == '':
        val = None
    return val


def check_number(val):
    """
        Проверка является ли строка числом
    """

    if val in ('.', '-', '+'):
        return None
    if bool(Regex_Number.match(str(val))):
        return val
    else:
        return None


def StrToBoolInt(text):
    return str_to_bool_int(text)


def str_to_bool_int(text):
    """
        перевод строк в булево значение
    """

    if text in ('Да', 'Нет', 'ДА', 'НЕТ', 'да', 'нет',
                'false', 'true', 'False', 'True', 'FALSE', 'TRUE', True, False):
        if text in ('Да', 'ДА', 'да', 'true', 'True', 'TRUE', True):
            return '1'
        if text in ('Нет', 'НЕТ', 'нет', 'false', 'False', 'FALSE', False):
            return '0'
    else:
        return text


def BarcodeToDic(wbdic, barcode, unit, factor=None, uweight=None,
                 ulength=None, uheight=None, uwidth=None, characteristic=None, characteristic_id=None):
    """
        Преобразование ШК в удобную для импрта структуру
    """

    barcode = barcode.strip()
    '''
        проверка на длинну ШК(пропускать только меньше 25)
        ШК не должен содержать пробелов
    '''

    if (len(barcode) > 25) or (' ' in barcode):
        return wbdic
    for itm in wbdic:
        if characteristic or characteristic_id:
            if not characteristic_id:
                if itm['unit'] == unit and itm['characteristic'] == characteristic:
                    itm['barcode'] = itm['barcode'] + ' ' + barcode
                    return wbdic
            else:
                if itm['unit'] == unit and itm['characteristic_id'] == characteristic_id:
                    itm['barcode'] = itm['barcode'] + ' ' + barcode
                    return wbdic
        else:
            if itm['unit'] == unit:
                itm['barcode'] = itm['barcode'] + ' ' + barcode
                return wbdic
    wbdic.append({'unit': unit, 'barcode': barcode, 'factor': factor, 'uweight': uweight,
                  'ulength': ulength, 'uheight': uheight, 'uwidth': uwidth,
                  'characteristic': characteristic, 'characteristic_id': characteristic_id})
    return wbdic


def unit_to_list(u_list, name):
    """
        Преобразование ед измерения в список
    """

    for itm in u_list:
        if itm == name:
            return u_list
    u_list.append(name)
    return u_list


def list_create_unique(name_list, name_itm):
    """
        Создание уникального списка
    """
    if name_list:
        for itm in name_list:
            if itm == name_itm:
                return name_list
    name_list.append(name_itm)
    return name_list


def formatMxDateTime(mxDateTime, format=None, id_system=None, sformatFrom='%Y-%m-%d %H:%M:%S'):
    if mxDateTime is None:
        return None
    sdate = str(mxDateTime).split('.')[0]
    sdateTuple = time.strptime(sdate, sformatFrom)
    if sdateTuple[5] == 60:
        sdateTuple = list(sdateTuple)
        sdateTuple[5] = 59
        sdateTuple = tuple(sdateTuple)
    return time.strftime(format, sdateTuple)


def TimeStampToDateTime(timestamp):
    if timestamp is None:
        return '&nbsp;'
    sdate = str(timestamp).split('.')[0]
    sformatFrom = '%Y-%m-%d %H:%M:%S'
    sdateTuple = time.strptime(sdate, sformatFrom)
    sformatTo = '%d.%m.%y %H:%M:%S'
    sdateStr = time.strftime(sformatTo, sdateTuple)
    return sdateStr


def TracebackLog(message=''):
    """
        Обработка исключений
    """

    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb_ = str(exc_value)
    tb = traceback.extract_tb(exc_traceback)
    for err in tb:
        tb_ += '\n'
        for er in err:
            tb_ += str(er) + ', '
    message = str(message) + '\n' + tb_ + "\n"
    message = decodeXStr(message)
    return message


def translit_to_ident(text, trunc_punctuation=False):
    """
        Транслитерация текста с русского и украинского алфавита в латиницу для преобразования в идентификаторы.
        Может использоваться при генерации имени слоя и логина.
        Если параметр truncPunctuation=True, то все символы, кроме буквенно-цифровых, урезаются,
        иначе эти символы (или любая их последовательность) заменяются одиночным символом подчеркивания (_).
    """

    ''' Словарик преобразований '''
    dic = {'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'YO', 'Ж': 'ZH',
           'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O',
           'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'KH', 'Ц': 'TS',
           'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Sch', 'Ъ': '',
           'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
           'Ґ': 'G',
           'Є': 'E',
           'Ї': 'I',
           'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i',
           'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
           'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
           'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
           'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
           'ґ': 'g',
           'є': 'e',
           'ї': 'yi',
           '0': '0',
           '1': '1',
           '2': '2',
           '3': '3',
           '4': '4',
           '5': '5',
           '6': '6',
           '7': '7',
           '8': '8',
           '9': '9'}

    result = ''
    for i in xrange(len(text)):
        if text[i] in dic:
            result += dic[text[i]]
        else:
            if not trunc_punctuation:
                if i == 0 or text[i - 1] in dic:
                    result += '_'
    return result


def unpack_file(file_name):
    """
        Распаковка файла архива
    """

    res = {}
    res['file_name'] = None
    res['message'] = None
    err = Zip_Errors[os.system('7z x "%s" -o"%s" -y' % (file_name, os.path.dirname(file_name)))]
    if len(err) == 0:
        res['file_name'] = get_first_file_mask(os.path.dirname(file_name), '*.xml')
    else:
        res['message'] = str(err)
    return res


def pack_file(file_name, file_pack):
    """
        Упаковка файла
    """

    result = True
    path = os.path.dirname(file_name)
    err = Zip_Errors[os.system('7z a "%s" "%s"' % (os.path.join(path, file_pack), file_name))]
    if len(err) == 0:
        try:
            os.unlink(file_name)
        except:
            result = False
    else:
        result = False
    return result


def pack_dir(dir_name, dir_pack):
    """
        Упаковка файла
    """

    result = True
    err = Zip_Errors[os.system('7z a "%s" "%s"' % (dir_name, dir_pack))]
    if len(err) == 0:
        pass
        '''try:
            os.unlink(dir_pack)
        except:
            result = False'''
    else:
        result = False
    return result


def get_first_file_mask(path, mask):
    """
        Получение самого старого файла в папке
    """

    file_list = sorted(glob.glob(path + '/' + mask), key=os.path.getmtime)
    for itm in file_list:
        return itm


def check_result(result, name_class):
    """
        Проверка результата класса
        использователься будет как декаратор.
        Если в классе пропустили обработку,
        то
    """

    if result != krconst.plugin_ok:
        raise NameError(krconst.m_e_programm_code_class % name_class)


def convert1cxml(inpfilexml):
    """
        Конвертация справочника сотрудников из 1С формата в RBS
    """

    output = {'from': None, 'to': None, 'insms': None, 'outsms': None, 'xml': None}

    str_itog = '<?xml version="1.0" encoding="UTF-8"?>' + '\n' + '\t' + '<root version="2.0">' + '\n' + '\t' + '\t' + '<dolgns>' + '\n'

    doc = minidom.parseString(inpfilexml)

    file_obmena = doc.childNodes[0]

    objects = [x for x in file_obmena.childNodes if x.nodeType == 1 and x.nodeName == u'Объект']

    people_objects = [
        x for x in objects
        if x.getAttribute(u'Тип') == u'СправочникСсылка.СотрудникиОрганизаций'
        and x.getAttribute(u'ИмяПравила') == u'СотрудникиОрганизаций'
    ]

    dolgn_objects = [
        x for x in objects
        if x.getAttribute(u'Тип') == u'СправочникСсылка.ДолжностиОрганизаций'
        and x.getAttribute(u'ИмяПравила') == u'ДолжностиОрганизаций'
    ]
    dict_dolgn = convert1cxml_get_dolgn(dolgn_objects)

    for dolgn in dict_dolgn:
        str_dolgn = '<dolgn dolgnguid="%s" dolgncode="%s" dolgnname="%s" />' \
                    % (dolgn['uid'], dolgn['code'], dolgn['name'])
        str_itog = str_itog + '\t' + '\t' + '\t'+ str_dolgn + '\n'

    str_itog = str_itog + '\t' + '\t' + '</dolgns>' + '\n' + '\t' + '\t' + '<mans>' + '\n'

    for ppl in people_objects:

        # dolgn
        dict_d = []
        name_d = ''
        #man
        dict_m = []
        fio_m = ''

        del_value = ''
        group_value = ''
        npp = ''

        #ФИО

        fio_m = convert1cxml_get_val(ppl, u'Наименование')

        #GUID and CODE, NAME DOLGN
        dict_d = convert1cxml_get_guid_code(ppl, u'ТекущаяДолжностьКомпании')
        name_d = convert1cxml_find_dolgn(dict_dolgn, dict_d['uid'])
        #GUID and CODE MAN
        dict_m = convert1cxml_get_guid_code(ppl, u'Физлицо')

        #deletemarker
        del_value = convert1cxml_get_val(ppl, u'ПометкаУдаления')

        #GROUP
        gr_nodes = [x for x in ppl.childNodes if x.nodeType == 1 and x.nodeName == u'Свойство' and x.getAttribute(u'Имя') == u'Физлицо']
        for node in gr_nodes:
            link_nodes = [x for x in node.childNodes if x.nodeType == 1 and x.nodeName == u'Ссылка']
            for link in link_nodes:
                group_value = convert1cxml_get_val(link, u'ЭтоГруппа')

        #POSITION
        pos_nodes = [x for x in ppl.childNodes if x.nodeType == 1 and x.nodeName == u'Свойство' and x.getAttribute(u'Имя') == u'Должность']
        for node in pos_nodes:
            link_nodes = [x for x in node.childNodes if x.nodeType == 1 and x.nodeName == u'Ссылка']
            for link in link_nodes:
                npp = link.getAttribute(u'Нпп').encode('utf-8')

        str_man = '<man realcode="%s" name="%s" deletemarker="%s" parentcode="%s" parent="%s" '\
                   'parentgroup="%s" group="%s" code="%s" position="%s" '\
                   'dolgnguid="%s" dolgncode="%s" dolgnname="%s"/>' \
                    % (dict_m['code'], fio_m, del_value, '', '',
                       '', group_value, dict_m['uid'], npp,
                       dict_d['uid'], dict_d['code'], name_d)

        str_itog = str_itog + '\t' + '\t' + '\t'+ str_man + '\n'

    str_itog = str_itog + '\t' + '\t' + '</mans>' + '\n' + '\t' + '</root>'

    data_objects = [x for x in file_obmena.childNodes if x.nodeType == 1 and x.nodeName == u'ДанныеПоОбмену']
    for x in data_objects:
        output['to'] = x.getAttribute(u'Кому').encode('utf-8')
        output['from'] = x.getAttribute(u'ОтКого').encode('utf-8')
        output['outsms'] = x.getAttribute(u'НомерИсходящегоСообщения').encode('utf-8')
        output['insms'] = x.getAttribute(u'НомерВходящегоСообщения').encode('utf-8')

    output['xml'] = str_itog

    return output


def convert1cxml_get_guid_code(xml, name):
    """
        Получение guid и code из файла 1С
    """

    #GUID and CODE DOLGN
    uid = ''
    code = ''
    id_nodes = [x for x in xml.childNodes if x.nodeType == 1 and x.nodeName == u'Свойство' and x.getAttribute(u'Имя') == name]
    for node in id_nodes:
        link_nodes = [x for x in node.childNodes if x.nodeType == 1 and x.nodeName == u'Ссылка']
        for link in link_nodes:
            uid = convert1cxml_get_val(link, u'{УникальныйИдентификатор}')
            code = convert1cxml_get_val(link, u'Код')

    result = {'uid': uid, 'code': code}
    return result


def convert1cxml_get_val(link, name):
    """
        Получение значения по имени
    """

    val = None

    id_nodess = [x for x in link.childNodes if x.nodeType == 1 and x.nodeName == u'Свойство' and x.getAttribute(u'Имя') == name]
    for id_node in id_nodess:
        id_leafs = [x for x in id_node.childNodes if x.nodeType == 1 and x.nodeName == u'Значение']
        for leaf in id_leafs:
            val = [x.nodeValue for x in leaf.childNodes if x.nodeType == 3][0].encode('utf-8')
    return val


def convert1cxml_get_dolgn(dolgn_objects):
    """
        Получение словаря с должностями
    """

    dict_dolgn = []
    for dlgn in dolgn_objects:
        link_nodes = [x for x in dlgn.childNodes if x.nodeType == 1 and x.nodeName == u'Ссылка']
        for link in link_nodes:
            uid = convert1cxml_get_val(link, u'{УникальныйИдентификатор}')
            code = convert1cxml_get_val(link, u'Код')
        name = convert1cxml_get_val(dlgn, u'Наименование')
        dict_dolgn.append({'uid': uid, 'code': code, 'name':name})
    return dict_dolgn


def convert1cxml_find_dolgn(dict_dolgn, uid):
    """
        Поиск наименования должности по guid
    """

    name_dolgn = ''
    for dolgn in dict_dolgn:
        if dolgn['uid'] == uid:
            name_dolgn = dolgn['name']
    return name_dolgn


def delete_files_by_mask(path, mask_files, date):
    """
        Удаление файлов по маске и дате изменения
    """

    res = None
    if not mask_files or not date or not path:
        return False
    for mask in mask_files.split(','):
        file_list = sorted(glob.glob(path + '/' + mask), key=os.path.getmtime)
        for itm in file_list:
            if os.path.isfile(itm):
                #получим дату создания и сравним
                if os.path.getmtime(itm) < date:
                    try:
                        os.unlink(itm)
                    except:
                        if not res:
                            res = 'Error unlinking old sticker file: ' + itm
                        else:
                            res = res + '/n Error unlinking old sticker file: ' + itm
                        continue
                else:
                    break
    return res


def current_time(db, layer_code='', time_zone=None):
    """
        Получение текущего времени с учетом time zone
    """
    if layer_code != '':
        if not time_zone:
            sql_text = 'select * from MY_GETDATETIME(?,?)'
            sql_params = [None, 'DT']
            res = db.dbExec(sql_text,
                            params=sql_params,
                            fetch='one')
            return res['DATETIMEZONE']
        else:
            return now() + timedelta(hours=time_zone)
    else:
        return now()

def current_time_zone(db, layer_code=''):
    """
        Получение текущей зоны
    """

    if layer_code != '':
        sql_text = 'select coalesce(cf.timezone,0) timezone from config cf'
        sql_params = []
        res = db.dbExec(sql_text,
                        params=sql_params,
                        fetch='one')
        return res['timezone']
    else:
        return 0
