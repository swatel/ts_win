# -*- coding: utf-8 -*-
# Данный плагин еще не готов
# -*- coding: cp1251-*

import poplib

import krconst
import BasePlugin as BP
import email
import mimetypes
import errno
import os
from optparse import OptionParser
#import queue_conf as cfg

class Plugin(BP.BasePlugin):
    def run(self):
        messagesInfo = None
        server = None
        try:
            # Добавить обработку
            cfg = self.XMLGetAllParams(self.taskparamsxml,True)        
            
            # server = poplib.POP3(cfg.email_server[0].mail_ip)
            # server.getwelcome()
            # server.user(cfg.email_server[0].mail_user)
            # server.pass_(cfg.email_server[0].mail_password)
            
            server = poplib.POP3_SSL(cfg['ServerName'],cfg['ServerPort'])
            server.getwelcome()
            server.user(cfg['UserName'])
            server.pass_(cfg['Password'])
            
            messagesInfo = server.list()[1]
        except:
           pass
        print('messagesInfo', messagesInfo)
        if messagesInfo:
            for msg in messagesInfo:
                msgNum = msg.split(" ")[0]
                full_message = "\n".join(server.retr(msgNum)[1])    
                msg = full_message
                message = email.message_from_string(msg)
                
                for part in message.walk(): #go to the end of the message
                    # print "Type = ", part.get_content_maintype()
                    # filename = part.get_filename()
                    # print "filename = ", filename
                    filename = part.get_filename()
                    if filename is not None:
                        fileQueueid = self.check_file_in_queue_sort(filename, 'CheckMail')
                        if fileQueueid:
                            #self.parent.k_conf.global_def_dir_tmp_files + 'Q' + str(fileQueueid) + '/'
                            dir = self.parent.k_conf.global_def_dir_tmp_files + 'Q' + str(fileQueueid) + '/'
                            if not os.access(dir, os.F_OK):
                                os.mkdir(dir)
                            fp = open(dir+filename,"wb")
                            fp.write(part.get_payload(decode=True))
                            fp.close()
                            self.LogFile(krconst.m_i_file_create_task_ok % filename)
                            self.update_status_turn_db(fileQueueid, krconst.kr_status_new)
                        #print "//---cc-----//"
                # sender = message.get('from', ('Unknown Sender'))        

                # #sender = parseaddr(sender)[1]
                # body = message.get_payload()
                # print '///////// -------------------------- /////////////////'
                # print sender
                # print '////////////--------------////////////////////////////'
                # if body != None:
                   # pass
                #server.dele(msgNum)
        if server:
            server.quit()        
 