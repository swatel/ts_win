# -*- coding: utf-8 -*-
import json
import krconst as c


class JsonReader(object):
    @classmethod
    def json_reads(cls, **args):
        """
        Создание модели из json
        @return: Новая модель
        """
        model = cls()
        model.json_read(**args)
        return model

    def json_read(self, json_obj=None, text=None, encoding='cp1251', prefix='', log_callback=None,
                  log_level=c.log_warning):
        """
        Загрузка значений из json
        @param json_obj: Объект JSON
        @param text: Текст JSON
        @param encoding: Кодировка
        @param prefix: Префикс колонок в json
        @param log_callback: Функция для обработки сообщений
        @param log_level: Уроверь логирования
        @return:
        """
        if json_obj is None and text is None:
            raise JsonReaderError('Отсутствуют JSON-данные.')
        elif json_obj is None:
            json_obj = json.loads(text, encoding=encoding)
        for name in self._columns:
            field = self._get_column(name)
            if prefix + name in json_obj:
                field.__set__(self, json_obj[prefix + name])
            else:
                field.__set__(self, None)
                if log_level in (c.log_warning, c.log_info):
                    message = 'JSON value of column "%s" is undefined' % field.name
                    if log_callback is not None:
                        log_callback(message)
                    else:
                        print(message)


class JsonReaderError(Exception):
    pass
