# -*- coding: utf-8 -*-
"""
    proper 13.03.2014
    version 0.0.0.1
    класс отправки почты
"""

import smtplib
import krconst as c
import os
import base64

import email.utils
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart # Модуль формирования сообщений из нескольких частей
from email.MIMEBase import MIMEBase # Модуль создания частей письма
from email.Encoders import encode_base64 # Модуль для кодирования присоединенных файлов

class Email():
    """
        класс отправки почты
    """

    parent_class = None

    smtp_server = None
    port = None
    username = None
    password = None
    from_address = None
    to_address = None
    use_tls = 0
    subject = None
    message = None
    path_file = None

    server = None

    #result_class = c.plugin_ok
    result_class = '1'

    def __init__(self, parent_class, **kwargs):
        """
            Инициализация атрибутов класса
        """

        self.parent_class = parent_class

        self.smtp_server = kwargs.get('smtp_server', None)
        self.port = kwargs.get('port', None)
        self.username = kwargs.get('username', None)
        self.password = kwargs.get('password', None)
        self.from_address = kwargs.get('from_address', None)
        self.use_tls = kwargs.get('use_tls', None)
        self.to_address = kwargs.get('to_address', None)
        self.to_address = self.to_address.replace(';', ',')
        self.to_address = self.to_address.split(',')
        self.subject = kwargs.get('subject', None)
        self.message = kwargs.get('message', None)
        self.path_file = kwargs.get('path_file', None)

        self.server = None

    def connect(self):
        """
            Подключение к серверу
        """

        try:
            self.server = smtplib.SMTP(self.smtp_server, self.port)
            ''' Переводим соединение в защищенный режим (Transport Layer Security) '''
            if self.use_tls == '1':
                self.server.starttls()
            ''' Проводим авторизацию '''
            self.server.login(self.username, self.password)
        except Exception as exc:
            message = c.m_e_smtp_connect % self.smtp_server
            message += ' ' + exc
            self.parent_class.log_file(message,
                                       terms=2,
                                       save_log_db=True)
            self.result_class = c.plugin_error
            return False
        return True

    def send_email(self):
        """
            Отправка почты
        """

        if self.connect():
            ''' формирование сообщения '''
            msg = 'From: %s\nTo: %s\r\nContent-Type: text/plain; charset="windows-1251"\r\nSubject: %s\n\n%s' \
                  % (self.from_address, ','.join(self.to_address), self.subject,
                     self.message)

            ''' отправка '''
            try:
                self.server.sendmail(self.from_address, self.to_address, msg)
            except:
                message = c.m_e_smtp_send % self.smtp_server
                self.parent_class.log_file(message,
                                           terms=2,
                                           save_log_db=True)
                self.result_class = c.plugin_error
                self.server.quit()
                return False
            self.server.quit()
            return True

    def send_email_with_file(self):
        """
            Отправка почты c файлом
        """

        if self.connect():
            msg = MIMEMultipart()
            msg['To'] = ', '.join(self.to_address)
            msg['From'] = email.utils.formataddr(('',
                                                  self.from_address))
            msg['Subject'] = self.subject

            body = MIMEText(self.message)
            msg.attach(body)

            if (self.path_file != None):
                path = os.path.abspath(self.path_file)
                fp = open(path, "rb")
                file_name = os.path.basename(path)

                to_attach = MIMEBase("application", "octet-stream")
                to_attach.set_payload(fp.read())
                encode_base64(to_attach)

                to_attach.add_header("Content-Disposition", "attachment", filename=file_name)
                fp.close()
                msg.attach(to_attach)

            try:
                self.server.sendmail(self.from_address, [self.to_address], msg.as_string())
            except:
                message = c.m_e_smtp_send % self.smtp_server
                self.parent_class.log_file(message,
                                           terms=2,
                                           save_log_db=True)
                self.result_class = c.plugin_error
                self.server.quit()
                return False
            self.server.quit()
            return True