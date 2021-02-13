# -*- coding: utf-8 -*-
import os
import sys
import re

def strInWin1251(str, flPrint=False):
    prev_ord = 0
    maybe1251 = False
    for simb in str:
        simb_ord = ord(simb)
        if flPrint:
            print simb, simb_ord
        if (127 < simb_ord < 144 or simb_ord == 145) and prev_ord == 209:
            return False
        if (123 < simb_ord < 192 or simb_ord == 129) and prev_ord == 208:
            return False            
        if maybe1251:
            return maybe1251
        if simb_ord == 208 or simb_ord == 209:
            maybe1251 = True
        else:
            if 223 < simb_ord < 256 or simb_ord == 184:  # lowercase
                return True
            elif 191 < simb_ord < 224 or simb_ord == 168:  # uppercase
                return True
            elif simb_ord == 185:  # Number sign
                return True                
        prev_ord = simb_ord
    return True
        
def textConvert(str):
    spl = str.split('\n')
    i = 0
    while i < len(spl):
        #flPrint = True if i == 3 else False
        #print spl[i], 'True' if strInWin1251(spl[i], flPrint) else 'False'
        if strInWin1251(spl[i], False):
            spl[i] = unicode(spl[i], 'windows-1251').encode('utf-8')
        i = i + 1
    return '\n'.join(spl)
        
        
def win1251ToUtf8(path):
  for dirs, subdirs, files in os.walk(path):
    for file in files:        
        filepath = os.path.join(str(dirs),str(file)).replace('\\','/')
        if file.endswith(".tmpl"):
            print filepath
            if(os.access(filepath,os.F_OK)):
                f = open(filepath, 'r')
                filetext = f.read()
                f.close()
                del f
                pos = filetext.find("\n")
                firstline = filetext[:(pos+1)]
                if firstline.find('#encoding') == -1:
                    filemodified = "#encoding utf-8\n" + textConvert(filetext)
                elif firstline.lower().find('utf-8') == -1:
                    filemodified = "#encoding utf-8\n" + textConvert(filetext[(pos+1):])
                else:
                    filemodified = None
                if filemodified:
                    f = open(filepath,'wt')
                    f.write(filemodified)
                    f.close()
                    del f
                else:
                    print "Not modified"
        elif file.endswith(".py"):
            print filepath
            filepath = os.path.join(str(dirs),str(file))
            spl = filepath.split('.')
            spl[-1] = 'tmpl'
            tmplpath = '.'.join(spl) 
            if not os.path.exists(tmplpath) and os.access(filepath,os.F_OK):
                f = open(filepath, 'r')
                filetext = f.read()
                f.close()
                del f
                pos = filetext.find("\n")
                firstline = filetext[:(pos+1)]
                if firstline.find('coding') == -1:
                    filemodified = "# -*- coding: utf-8 -*-\n" + textConvert(filetext)
                elif firstline.lower().find('utf-8') == -1:
                    filemodified = "# -*- coding: utf-8 -*-\n" + textConvert(filetext[(pos+1):])
                else:
                    filemodified = None
                if filemodified:
                    f = open(filepath,'wt')
                    f.write(filemodified)
                    f.close()
                    del f
                else:
                    print "Not modified"
        elif file.endswith(".js") or file.endswith(".css") or file.endswith(".html"):
            print filepath
            filepath = os.path.join(str(dirs),str(file))
            if os.access(filepath,os.F_OK):
                f = open(filepath, 'r')
                filetext = f.read()
                f.close()
                del f
                filemodified = textConvert(filetext)
                f = open(filepath,'wt')
                f.write(filemodified)
                f.close()
                del f
                print "Converted"


path = os.path.abspath("")
win1251ToUtf8(path)