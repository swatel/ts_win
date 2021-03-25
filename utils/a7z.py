# -*- coding: cp1251-*
"""
    23.03.2015
    Утилиты работы с архивом
"""

import os

import utils.file as f

ZIP_ERRORS = {
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


def unpack_file(file_name):
    """
        Распаковка файла архива
    """

    res = dict()
    res['file_name'] = None
    res['message'] = None
    cmd_str = '7z x "%s" -o"%s" -y' % (file_name, os.path.dirname(file_name))
    err = ZIP_ERRORS[os.system(cmd_str)]
    if len(err) == 0:
        res['file_name'] = f.get_first_file_mask(os.path.dirname(file_name),
                                                 '*.xml')
    else:
        res['message'] = str(err)
    return res


def pack_file(file_name, file_pack):
    """
        Упаковка файла
    """

    result = True

    path = os.path.dirname(file_name)
    cmd_str = '7z a "%s" "%s"' % (os.path.join(path, file_pack), file_name)
    err = ZIP_ERRORS[os.system(cmd_str)]
    if len(err) == 0:
        try:
            os.unlink(file_name)
        except ValueError:
            result = False
    else:
        result = False
    return result


def pack_dir(dir_name, dir_pack):
    """
        Упаковка файла
    """

    result = True
    cmd_str = '7z a "%s" "%s"' % (dir_name, dir_pack)
    err = ZIP_ERRORS[os.system(cmd_str)]
    if len(err) == 0:
        pass
    else:
        result = False
    return result
