# -*- coding: cp1251-*
"""
    24.03.2015
    Декораторы
"""

from datetime import datetime
import krconst as c


def timer_sql(func):
    """
        Декоратов выполнения запросов для базового плагина
    """

    def tmp(*args, **kwargs):
        """
            Обертка
        """

        base_plugin = args[0]
        if base_plugin.logextended in ('1', '2'):
            date_start_sql = datetime.today()
        res = func(*args, **kwargs)
        if base_plugin.logextended in ('1', '2'):
            delta_date_start_sql = datetime.today() - date_start_sql
            try:
                sqlparams = kwargs['sql_params']
            except KeyError:
                sqlparams = ''
            text = c.sql_timer % {'sql': args[1],
                                  'sqlparams': sqlparams,
                                  'delta_date_start_sql': delta_date_start_sql}
            base_plugin.log_file(text + c.kr_term_enter)
        return res
    return tmp


def timer(func):
    """
        Декорат измерения времени выполнения метода плагина
        (или класса, у которого есть self.parent_obj, указывающий на плагин)
    """
    import BasePlugin as bp

    def tmp(*args, **kwargs):
        """
            Обертка
        """
        _self = args[0]
        base_plugin = None
        if isinstance(_self, bp.BasePlugin):
            base_plugin = _self
        else:
            if hasattr(_self, 'parent_obj') and isinstance(_self.parent_obj, bp.BasePlugin):
                base_plugin = _self.parent_obj
        if base_plugin is None:
            res = func(*args, **kwargs)
        else:
            log_extended = base_plugin.logextended in ('1', '2')
            if log_extended:
                date_start = datetime.today()
            res = func(*args, **kwargs)
            if log_extended:
                delta_date_start = datetime.today() - date_start
                name = func.__name__
                text = c.timer_format % {'name': name,
                                         'delta_date_start': delta_date_start}
                base_plugin.log_file(text + c.kr_term_enter)
        return res
    return tmp


def synchronized(lock):
    """ Synchronization decorator. """

    def wrap(f):
        def exec_func(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()
        return exec_func
    return wrap
