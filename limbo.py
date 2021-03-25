# -*- coding: windows-1251 -*-
import os
from glob import glob
from shutil import rmtree, move
from datetime import datetime

version = '0.1'

"""
�������:
orm/unittest/temp_report2.txt - �������� ����� �� �����
orm/unittest/*.xml - �������� ������ �� �����
#error.log - �����������
examples/bank/In/* - �������� ����������� ��������
examples/bank/Out - �������� ����������� �������� ������ � ���������
"""

ERROR = 'ERROR'
WARNING = 'WARNING'
INFO = 'INFO'


def limbo(path, request):
    """
    ������� ������
    @param path: �������� �����
    @param request: ���� �������� �� ��������
    @return:
    """
    if os.path.exists(request) and os.path.isfile(request):
        info('������� ������. '
             '��������� ����� "' + request + '".')
    else:
        warning('������� ������. '
                '���� "' + request + '" �� ������ (���� ������� �� ���������, ����������� ��� ���������).')
        return True
    request_path, request_filename = os.path.split(request)
    basename, extension = os.path.splitext(request_filename)
    # ��������� ����
    request_temp = os.path.join(request_path, basename + '.tmp')
    with open(request, 'r') as r:
        with open(request_temp, 'w') as w:
            for line in r:
                line = line.rstrip('\n')
                mask_split = line.split('#')
                mask = mask_split[0].strip(' ') if len(mask_split) > 0 else ''
                if mask == '':
                    if line != '':
                        # ���� ������ �� ������, ��������
                        keep(w, line)
                else:
                    limbo_path = os.path.join(path, mask)
                    if mask.find('*') > -1:
                        # ������ � ������
                        matches = glob(limbo_path)
                        if len(matches) == 0:
                            keep(w, line)
                            warning('�� ����� "' + limbo_path + '" ������ �� �������.')
                        else:
                            for match in matches:
                                pass_through_the_limbo(w, line, path, match)
                    else:
                        # ������ � ����� ������/���������
                        pass_through_the_limbo(w, line, path, limbo_path)
    move(request_temp, request)
    return True


def pass_through_the_limbo(f, line, path, limbo_path):
    try:
        if is_subdir(path, limbo_path):
            if os.path.exists(limbo_path):
                remove(limbo_path)
            else:
                keep(f, line)
                warning('���� "' + limbo_path + '" �� ������.')
        else:
            keep(f, line)
            warning('���� "' + limbo_path + '" ������� �� ������� ��������� ��������.')
    except Exception as e:
        keep(f, line)
        error(e.message)


def is_subdir(path, sub_path):
    """
    ��������, �������� �� ������� ���������
    @param path: �������� �������
    @param sub_path: ��������� �������
    @return: Boolean
    """
    # path = os.path.normpath(path)
    # directory = os.path.normpath(directory)
    path = os.path.realpath(path)
    sub_path = os.path.realpath(sub_path)
    relative = os.path.relpath(sub_path, path)
    return not relative.startswith(os.pardir + os.sep)


def keep(f, line):
    f.write(line + '\n')
    info('������ "' + line + '" ���������.')


def remove(path):
    if os.path.isdir(path):
        rmtree(path)
        info('������� "' + path + '" ������.')
    else:
        os.remove(path)
        info('���� "' + path + '" ������.')


def info(text):
    message(INFO, text)


def warning(text):
    message(WARNING, text)


def error(text):
    message(ERROR, text)


def message(prefix, text):
    print str(datetime.now()) + ' ' + prefix + ': ' + text
