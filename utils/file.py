# -*- coding: cp1251-*
"""
    23.03.2015
    ������� ������ � ������� � ������������
"""

import os
import glob


def get_first_file_mask(path, mask):
    """
        ��������� ������ ������� ����� � �����
    """

    file_list = sorted(glob.glob(path + '/' + mask),
                       key=os.path.getmtime)
    for itm in file_list:
        return itm


def check_dir_by_path(path, sub_folders, mask_files='*', ignore_path=' ', mtime_from=None, mtime_to=None,
                      ignore_file=' '):
    """
    �������� �������� �� ������� ����� �� ����� �����
    @param path: ���� � �����
    @param sub_folders: ��������� �� �����������
    @param mask_files: ����� �����
    @param ignore_path: ������������ ����
    @param mtime_from: ����������� �� ���� ��������� ����� (�) unix_timestamp
    @param mtime_to: ����������� �� ���� ��������� ����� (��) unix_timestamp
    @param ignore_file: ������������ �����
    @return:
    """

    res_file_list = []
    for mask in mask_files.split(','):
        file_list = sorted(glob.glob(path + '/' + mask), key=os.path.getmtime)
        for itm in file_list:
            if not itm.endswith('/' + ignore_path) and not itm.endswith('\\' + ignore_path):
                if os.path.isfile(itm):
                    itm_replaced = itm.replace('\\', '/')
                    if mtime_from is not None or mtime_to is not None:
                        mtime = os.path.getmtime(itm)
                        mtime_accepted = (mtime_from is None or mtime >= mtime_from) \
                                         and (mtime_to is None or mtime <= mtime_to)
                        if mtime_accepted:
                            res_file_list.append(itm_replaced)
                    else:
                        file_name = os.path.basename(itm_replaced)
                        if not file_name.startswith(ignore_file):
                            res_file_list.append(itm_replaced)
                elif sub_folders == '1':
                    if os.path.isdir(itm):
                        tmp_file_list = check_dir_by_path(itm,
                                                          sub_folders,
                                                          mask_files=mask_files,
                                                          ignore_path=ignore_path,
                                                          mtime_from=mtime_from,
                                                          mtime_to=mtime_to,
                                                          ignore_file=ignore_file)
                        if tmp_file_list:
                            for itm_file in tmp_file_list:
                                res_file_list.append(itm_file.replace('\\', '/'))
    return res_file_list


def delete_tmp_file(full_file_name, delete_dir=False):
    """
    �������� ����� + ���������� ���� �����, ���� ��� �����
    @param full_file_name: ��� �����
    @param delete_dir: ������� �������� ���������� ���� ��� �����
    @return: ���������� ��������
    """
    if os.access(full_file_name, os.F_OK):
        try:
            os.unlink(full_file_name)
        except:
            raise
            return False
    if delete_dir:
        ''' �������� ���� �� � �������� ��� �����'''
        file_dir = os.path.dirname(full_file_name)
        try:
            if not os.listdir(file_dir):
                import shutil
                shutil.rmtree(file_dir)
        except:
            raise
            return False

    return True
