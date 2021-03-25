# -*- coding: windows-1251 -*-
version = '0.0.3.0'

import kqkernel as k

import rbsqutils as rqu

try:
    a = k.Layer('ENGINE_LITEBOX', 'FREE1')
except:
    print rqu.TracebackLog('')