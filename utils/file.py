# -*- coding: cp1251-*
"""
    23.03.2015
    Утилиты работы с файлами и директориями
"""

import os
import glob


def get_first_file_mask(path, mask):
    """
        Получение самого старого файла в папке
    """

    file_list = sorted(glob.glob(path + '/' + mask),
                       key=os.path.getmtime)
    for itm in file_list:
        return itm


def check_dir_by_path(path, sub_folders, mask_files='*', ignore_path=' ', mtime_from=None, mtime_to=None,
                      ignore_file=' '):
    """
    Проверка каталога на наличие файла по маске файла
    @param path: путь к файлу
    @param sub_folders: проверять ли подкаталоги
    @param mask_files: маска файла
    @param ignore_path: игнорируемый путь
    @param mtime_from: ограничение на дату изменения файла (с) unix_timestamp
    @param mtime_to: ограничение на дату изменения файла (по) unix_timestamp
    @param ignore_file: игнорируемые файлы
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
    Удаление файла + директории если нужно, если она пуста
    @param full_file_name: имя файла
    @param delete_dir: признак удаления директории если она пуста
    @return: успешность действие
    """
    if os.access(full_file_name, os.F_OK):
        try:
            os.unlink(full_file_name)
        except:
            raise
            return False
    if delete_dir:
        ''' проверим есть ли в каталоге еще файлы'''
        file_dir = os.path.dirname(full_file_name)
        try:
            if not os.listdir(file_dir):
                import shutil
                shutil.rmtree(file_dir)
        except:
            raise
            return False

    return True
