# -*- coding: utf-8 -*-
from orm_v2.SQLColumnDef import *


class SQLColumn(object):
    """
    Основано на Col -- SQLObject columns
    """

    base_class = SQLColumnDef

    def __init__(self, name, **kw):
        self.__dict__['_name'] = name
        self.__dict__['_kw'] = kw
        self.__dict__['_extra_vars'] = {}
        self.__dict__['_value'] = None

    def new_instance(self):
        return self.base_class(name=self._name, extra_vars=self._extra_vars, **self._kw)

    def _set_name(self, value):
        assert self._name is None or self._name == value, (
            "You cannot change a name after it has already been set "
            "(from %s to %s)" % (self.name, value))
        self.__dict__['_name'] = value

    def _get_name(self):
        return self._name

    name = property(_get_name, _set_name)


class StringColumn(SQLColumn):

    base_class = StringColumnDef


class CharColumn(StringColumn):

    base_class = CharColumnDef


class IntColumn(SQLColumn):

    base_class = IntColumnDef


class DoubleColumn(SQLColumn):

    base_class = DoubleColumnDef


class DoubleCommaColumn(SQLColumn):

    base_class = DoubleCommaColumnDef


class TimestampColumn(SQLColumn):

    base_class = TimestampColumnDef


class ForeignKey(SQLColumn):
    pass


class IntForeignKey(ForeignKey):

    base_class = IntForeignKeyDef


class DoubleForeignKey(ForeignKey):

    base_class = DoubleForeignKeyDef


class DetailColumn(SQLColumn):

    base_class = DetailColumnDef
