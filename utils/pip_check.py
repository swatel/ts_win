# -*- coding: cp1251-*
import contextlib


@contextlib.contextmanager
def std_capture():
    """
    �������� ������������ ������ � ������
    :return: list [0] - ���������� stdout
             list [1] - ���������� stderr
    """
    import sys
    from cStringIO import StringIO
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
    �������� �� ������������� ������, ���� ��� - �� ������� ���������� ����� pip
    :param package: ��� ������
    :param transport: ������������ ������, ��� ��������������
    :param install: �������������, ���� �� ������
    :return: Boolean
    """
    import imp
    try:
        imp.find_module(package)
        return True  # ������ ���� - �������
    except ImportError:
        transport.log_file('������ ' + package + ' �� ������')
        if not install:  # ���� �� �������� ������������� - �����
            return False
    transport.log_file('������� ���������� ����� pip')
    # ���������/������� pip ����� easy_install
    # ���� ������� ���������� ����� imp.find_module('pip') - ����� �������� ������ ������
    try:
        from setuptools.command import easy_install
        easy_install.main(['-U', 'pip'])
    except:
        transport.log_file('�� ������� ����������/�������� pip', 0, 'ERROR')
        return False
    # ������ �� �����, �������� ����������
    try:
        imp.find_module('pip')
    except ImportError:
        transport.log_file('�� ������ pip', 0, 'ERROR')
        return False
    # ������� ����������, pip ��� ������ ����
    try:
        import pip, sys
        # ������������� ����������� ����� � ������
        with std_capture() as out:
            res = pip.main(['install', package])
        if res != 0:
            message = out[0]
            error_message = out[1]
            import os
            transport.log_file('�� ������� ���������� ������ ' + package, 0, 'ERROR')
            if error_message:
                transport.log_file(error_message, 0, 'ERROR')
            if message:
                transport.log_file(message)
            return False
        else:
            transport.log_file('������ ' + package + ' ����������')
    except:
        transport.log_file('�� ������� ���������� ������ ' + package, 0, 'ERROR')
        return False
    # ��������� ������� ��������� ������
    try:
        imp.find_module(package)
        return True  # ������ ���� - �������
    except ImportError:
        transport.log_file('������ ' + package + ' �� ������', 0, 'ERROR')
        return False
