# -*- coding: utf-8 -*-
import os
from glob import glob
from shutil import rmtree, move
from datetime import datetime

version = '0.1'

"""
Примеры:
orm/unittest/temp_report2.txt - удаление файла по имени
orm/unittest/*.xml - удаление файлов по маске
#error.log - комментарий
examples/bank/In/* - удаление содержимого каталога
examples/bank/Out - удаление содержимого каталога вместе с каталогом
"""

ERROR = 'ERROR'
WARNING = 'WARNING'
INFO = 'INFO'


def limbo(path, request):
    """
    Очистка файлов
    @param path: Корневая папка
    @param request: Файл запросов на удаление
    @return:
    """
    if os.path.exists(request) and os.path.isfile(request):
        info('Очистка файлов. '
             'Обработка файла "' + request + '".')
    else:
        warning('Очистка файлов. '
                'Файл "' + request + '" не найден (если очистка не требуется, игнорируйте это сообщение).')
        return True
    request_path, request_filename = os.path.split(request)
    basename, extension = os.path.splitext(request_filename)
    # Временный файл
    request_temp = os.path.join(request_path, basename + '.tmp')
    with open(request, 'r') as r:
        with open(request_temp, 'w') as w:
            for line in r:
                line = line.rstrip('\n')
                mask_split = line.split('#')
                mask = mask_split[0].strip(' ') if len(mask_split) > 0 else ''
                if mask == '':
                    if line != '':
                        # Если строка не пустая, сохраним
                        keep(w, line)
                else:
                    limbo_path = os.path.join(path, mask)
                    if mask.find('*') > -1:
                        # Работа с маской
                        matches = glob(limbo_path)
                        if len(matches) == 0:
                            keep(w, line)
                            warning('По маске "' + limbo_path + '" ничего не найдено.')
                        else:
                            for match in matches:
                                pass_through_the_limbo(w, line, path, match)
                    else:
                        # Работа с одним файлом/каталогом
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
                warning('Путь "' + limbo_path + '" не найден.')
        else:
            keep(f, line)
            warning('Путь "' + limbo_path + '" выходит за пределы корневого каталога.')
    except Exception as e:
        keep(f, line)
        error(e.message)


def is_subdir(path, sub_path):
    """
    Проверка, является ли каталог вложенным
    @param path: Основной каталог
    @param sub_path: Вложенный каталог
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
    info('Строка "' + line + '" сохранена.')


def remove(path):
    if os.path.isdir(path):
        rmtree(path)
        info('Каталог "' + path + '" удален.')
    else:
        os.remove(path)
        info('Файл "' + path + '" удален.')


def info(text):
    message(INFO, text)


def warning(text):
    message(WARNING, text)


def error(text):
    message(ERROR, text)


def message(prefix, text):
    print(str(datetime.now()) + ' ' + prefix + ': ' + text)
