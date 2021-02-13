# -*- coding: utf-8 -*-
# Данный плагин еще не готов

import poplib

import krconst
import BasePlugin as BP
import email
import mimetypes
import errno
import os
from optparse import OptionParser
import xml.etree.ElementTree as et
#import queue_conf as cfg


class Plugin(BP.BasePlugin):
    def run(self):      
        SQLtext = None
        #добавить обработку 
        params = self.XMLGetAllParams(self.task['export_sql'],True)
        if('SQLtext' in params):
            SQLtext = params['SQLtext']
        #print SQLtext
        que_params = self.resqueue['queueparams']
        res = None       
        
        if len(que_params) == 0:
            res = self.db.dbExec(SQLtext,params=(), fetch='many')
        else:
            pass
            #print "Queue params are all in string ", que_params            
        
        filename_xml = "D:/ser_repa/ROBOT/tmp/expsql.xml"
        if (res):
            res_list = []            
            if(len(res) > 0):
                fp = open(filename_xml, "w")
                fp.write("<?xml version='1.0' charset='windows-1251'?><commands>")
                for item in res:
                    cmd = "<commandline s='" + str(item["s"]) + "' msg='"+str(item["msg"]) + "' cond='" + str(item["cond"]) + "' />"
                    fp.write(cmd)
                    
                fp.write("</commands>");
                fp.close()
                print("File was succesfully created. Doing post job...")
            actparams = None
            if self.queueactionsparamsxml is not None:
                actparams = self.queueactionsparamsxml
            if self.task['export_actparams']:
                actparams = self.task['export_actparams']
            
            if actparams is not None:
                self.export_post_job(filename_xml, actparams)