# -*- coding: utf-8 -*-
import contextlib


@contextlib.contextmanager
def std_capture():
    """
    Перехват стандартного вывода и ошибок
    :return: list [0] - содержимое stdout
             list [1] - содержимое stderr
    """
    import sys
    from io import StringIO
    oldout, olderr = sys.stdout, sys.stderr
    try:
        out = [StringIO(), StringIO()]
        sys.stdout, sys.stderr = out
        yield out
    finally:
        sys.stdout, sys.stderr = oldout, olderr
        out[0] = out[0].getvalue()
        out[1] = out[1].getvalue()


def pip_check(package, transport, install=False):
    """
    Проверка на существование модуля, если нет - то попытка установить через pip
    :param package: Имя модуля
    :param transport: Транспортный модуль, для журналирования
    :param install: Устанавливать, если не найден
    :return: Boolean
    """
    import imp
    try:
        imp.find_module(package)
        return True  # Модуль есть - выходим
    except ImportError:
        transport.log_file('Модуль ' + package + ' не найден')
        if not install:  # Если не пытаться устанавливать - выход
            return False
    transport.log_file('Попытка установить через pip')
    # Установим/обновим pip через easy_install
    # Если сделать обновление после imp.find_module('pip') - будет работать старая версия
    try:
        from setuptools.command import easy_install
        easy_install.main(['-U', 'pip'])
    except:
        transport.log_file('Не удалось установить/обновить pip', 0, 'ERROR')
        return False
    # Модуль не нашли, пытаемся установить
    try:
        imp.find_module('pip')
    except ImportError:
        transport.log_file('Не найден pip', 0, 'ERROR')
        return False
    # Пробуем установить, pip уже должен быть
    try:
        import pip, sys
        # Перехватываем стандартный вывод и ошибки
        with std_capture() as out:
            res = pip.main(['install', package])
        if res != 0:
            message = out[0]
            error_message = out[1]
            import os
            transport.log_file('Не удалось установить модуль ' + package, 0, 'ERROR')
            if error_message:
                transport.log_file(error_message, 0, 'ERROR')
            if message:
                transport.log_file(message)
            return False
        else:
            transport.log_file('Модуль ' + package + ' установлен')
    except:
        transport.log_file('Не удалось установить модуль ' + package, 0, 'ERROR')
        return False
    # Последняя попытка загрузить модуль
    try:
        imp.find_module(package)
        return True  # Модуль есть - выходим
    except ImportError:
        transport.log_file('Модуль ' + package + ' не найден', 0, 'ERROR')
        return False
