# -*- coding: utf-8 -*-
#import os.path
#os.path.dirname(os.path.abspath(__file__))
#print sys.path
#sys.path.append("..") 

import BasePlugin as BP

class Plugin(BP.BasePlugin):
    def run(self):
        if self.pxml:            
            pass
            #CommandText = self.ParserXML('CommandText')
            #self.result = self.ExecuteSQL(CommandText)