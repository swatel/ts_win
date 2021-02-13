# -*- coding: utf-8 -*-

import os

import kqkernel as k
import rbsqutils as rqu
from limbo import limbo

version = '1.0.1.9'

# очистка файлов
print('cleanup start')
try:
    limbo(os.getcwd(), './cleanup.txt')
    print('cleanup finish')
except Exception as exc:
    print('cleanup error', str(exc))


print('servercode=TASKSERVER')
try:
    a = k.QKernel('USR', 'TASKSERVER')
except:
    print(rqu.TracebackLog(''))