# -*- coding: windows-1251 -*-
version = '0.0.0.0'

import kqkernel as k

import rbsqutils as rqu

print('servercode=TS_WIN')
try:
    a = k.QKernel('3Pine', 'TS_WIN')
except:
    print (rqu.TracebackLog(''))