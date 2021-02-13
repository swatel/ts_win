# -*- coding: utf-8 -*-

"""
    Модуль для работы с xml
"""

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '30.04.2015'

#import xml.etree.ElementTree as Et
import krconst as c


def xml_get_value_by_attr(xml, attr, default=None):
    """
        Значение параметра по атрибуту
        @param xml: строка
        @param attr: имя атрибута
        @param default: значение по умолчанию, если значения нет или пустой атрибут
        @return: result = {'value': None, 'message': '', 'error': 0}
            value - значение
            message - сообщение
            error - 0 - ошибки нет
                    1 - не критическая ошибка
                    2 - критическая ошибка
    """

    result = {'value': None, 'message': '', 'error': 0}
    if not attr:
        result['message'] = c.m_e_exec_method % 'xml_get_value_by_attr. ' + c.m_e_xml_not_attr
        result['error'] = 2
    else:
        value = xml.get(attr.decode("cp1251"))

        # проверка на пустую дату которая может прийти из 1С
        if value == '01.01.0001 0:00:00':
            value = None
        if value == '':
            value = None
        if value is not None:
            value = value.encode("cp1251", 'ignore')
        else:
            value = default
        result['value'] = value
    return result


def xml_value(xml, key):
    """
        Поиск значения по ключу
    """

    try:
        text = xml.find(key).text
    except:
        text = None
    if text:
        text = text.encode('cp1251', 'ignore')
    return text


def xml_attrib(xml, key, attrib):
    """
        Поиск атрибута по ключу
    """

    try:
        text = xml.find(key).attrib[attrib]
    except:
        text = None
    if text:
        text = text.encode('cp1251', 'ignore')
    return text