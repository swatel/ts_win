# -*- coding: utf-8 -*-
import mx.DateTime as mxd

OneToMany = 2
OneToOne = 1


class SQLColumnDef(object):
    """
    Основано на SOCol -- SQLObject columns
    """

    value = None

    def __init__(self,
                 name,
                 primary_key=False,
                 extra_vars=None,
                 foreign_key=None,
                 real_column=True,
                 column_prefix='',
                 detail_key=None,
                 detail_relation=None
                 # creationOrder,
                 # dbName=None,
                 # default=NoDefault,
                 # defaultSQL=None,
                 # alternateID=False,
                 # alternateMethodName=None,
                 # constraints=None,
                 # notNull=NoDefault,
                 # notNone=NoDefault,
                 # unique=NoDefault,
                 # sqlType=None,
                 # columnDef=None,
                 # validator=None,
                 # validator2=None,
                 # immutable=False,
                 # cascade=None,
                 # lazy=False,
                 # noCache=False,
                 # forceDBName=False,
                 # title=None,
                 # tags=[],
                 # origName=None,
                 # dbEncoding=None
                 ):
        """

        @param name:
        @param primary_key:
        @param extra_vars:
        @param foreign_key: orm.SQLModel
        @return:
        """

        super(SQLColumnDef, self).__init__()
        assert name, 'Имя колонки обязательно для заполнения'
        self.__dict__['_extra_vars'] = {}
        self.name = name
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.real_column = real_column
        self.column_prefix = column_prefix
        self.detail_key = detail_key
        self.detail_relation = detail_relation

    @classmethod
    def validate(cls, value):
        """
        Валидация данных
        @param value:
        @return:
        """
        return value

    def __get__(self, instance, type=None):
        """
        Дексриптор - получение значения
        @param instance:
        @param type:
        @return:
        """
        if instance is None:
            # class attribute, return the descriptor itself
            return self
        return self.value

    def __set__(self, instance, value):
        """
        Дексриптор - установка значения
        @param instance:
        @param value:
        @return:
        """
        self.value = self.validate(value)

    def __delete__(self, obj):
        raise AttributeError("I can't be deleted from %r" % obj)


class StringColumnDef(SQLColumnDef):
    data_type = str

    @classmethod
    def validate(cls, value):
        value = super(StringColumnDef, cls).validate(value)
        if value is None:
            return value
        if isinstance(value, str):
            return value.encode('cp1251')
        if not isinstance(value, cls.data_type):
            try:
                value = str(value)
            except ValueError:
                raise AttributeError('Could not convert the value to string')
        return value


class CharColumnDef(StringColumnDef):
    pass


class IntColumnDef(SQLColumnDef):
    data_type = int

    @classmethod
    def validate(cls, value):
        value = super(IntColumnDef, cls).validate(value)
        if value is None:
            return value
        if not isinstance(value, cls.data_type):
            try:
                value = int(value)
            except ValueError:
                raise AttributeError('Could not convert the value % to int' % str(value))
        return value


class DoubleColumnDef(SQLColumnDef):
    data_type = float

    @classmethod
    def validate(cls, value):
        value = super(DoubleColumnDef, cls).validate(value)
        if value is None:
            return value
        if not isinstance(value, cls.data_type):
            try:
                value = float(value)
            except ValueError:
                raise AttributeError('Could not convert the value %s to float' % str(value))
        return value


class DoubleCommaColumnDef(DoubleColumnDef):
    pass


class TimestampColumnDef(SQLColumnDef):
    data_type = mxd.DateTimeType

    @classmethod
    def validate(cls, value):
        value = super(TimestampColumnDef, cls).validate(value)
        if value is None:
            return value
        if not isinstance(value, cls.data_type):
            if isinstance(value, str):
                try:
                    value = mxd.DateTimeFrom(value)
                except ValueError:
                    raise AttributeError('Could not convert the value %s to datetime' % str(value))
            else:
                raise AttributeError('Could not convert the value to datetime')
        return value


class ForeignKeyDef(SQLColumnDef):
    def fk_model(self, execute_sql_func):
        return self.foreign_key.get(self.value, execute_sql_func)


class IntForeignKeyDef(ForeignKeyDef):
    base_def = IntColumnDef


class DoubleForeignKeyDef(ForeignKeyDef):
    base_def = DoubleColumnDef


class DetailColumnDef(SQLColumnDef):
    def get_details(self, execute_sql_func, sql_text, sql_params):
        return self.detail_key.gets(execute_sql_func, sql_text, sql_params)
