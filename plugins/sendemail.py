# -*- coding: utf-8 -*-
"""
    swat 05.05.2017
    version 0.0.0.1
    класс отправки почты с файлом
"""

import BasePlugin as BP
import rbssendemail as e


class Plugin(BP.BasePlugin):
    """
        класс работы с почтой
    """

    def run(self):
        """
            отправка почты
        """
        toaddress = self.ParserXML(self.queueparamsxml, 'toaddress')
        if toaddress is None:
            toaddress = self.ParserXML(self.taskparamsxml, 'to_address')
        subject = self.ParserXML(self.queueparamsxml, 'subject')
        if subject is None:
            subject = self.ParserXML(self.taskparamsxml, 'subject')
        path_file = self.ParserXML(self.queueparamsxml, 'path_file')
        if path_file is None:
            path_file = self.ParserXML(self.taskparamsxml, 'path_file')
        a = e.Email(self,
                    smtp_server=self.ParserXML(self.taskparamsxml, 'smtp_server'),
                    port=self.ParserXML(self.taskparamsxml, 'port'),
                    username=self.ParserXML(self.taskparamsxml, 'username'),
                    password=self.ParserXML(self.taskparamsxml, 'password'),
                    from_address=self.ParserXML(self.taskparamsxml, 'from_address'),
                    use_tls=self.ParserXML(self.taskparamsxml, 'usetls'),
                    to_address=toaddress,
                    subject=subject,
                    message=self.ParserXML(self.queueparamsxml, 'message').replace('#1', '\n'),
                    path_file=path_file)
        a.send_email_with_file()
