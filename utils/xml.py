# -*- coding: cp1251-*

"""
    ������ ��� ������ � xml
"""

__author__ = 'swat'
VERSION = '0.0.3.0'
DATE_VERSION = '30.04.2015'

#import xml.etree.ElementTree as Et
import krconst as c


def xml_get_value_by_attr(xml, attr, default=None):
    """
        �������� ��������� �� ��������
        @param xml: ������
        @param attr: ��� ��������
        @param default: �������� �� ���������, ���� �������� ��� ��� ������ �������
        @return: result = {'value': None, 'message': '', 'error': 0}
            value - ��������
            message - ���������
            error - 0 - ������ ���
                    1 - �� ����������� ������
                    2 - ����������� ������
    """

    result = {'value': None, 'message': '', 'error': 0}
    if not attr:
        result['message'] = c.m_e_exec_method % 'xml_get_value_by_attr. ' + c.m_e_xml_not_attr
        result['error'] = 2
    else:
        value = xml.get(attr.decode("cp1251"))

        # �������� �� ������ ���� ������� ����� ������ �� 1�
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
        ����� �������� �� �����
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
        ����� �������� �� �����
    """

    try:
        text = xml.find(key).attrib[attrib]
    except:
        text = None
    if text:
        text = text.encode('cp1251', 'ignore')
    return text