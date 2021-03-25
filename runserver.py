# -*- coding: windows-1251 -*-
version = '0.0.0.0'

import kqkernel as k

import rbsqutils as rqu

#UR
#print 'servercode=RPYTHON'
#a = k.QKernel('SWAT-BOOK', 'RPYTHON')
#DONTEXPROM
#print 'servercode=TASKSERVER'
#a = k.QKernel('DONTEXPROM', 'TASKSERVER')
#a = k.QKernel('DONTEXPROMSERVER', 'TASKSERVER')

#MYSHOP
#print 'servercode=TASKSERVER'
#a = k.QKernel('MYSHOP', 'TASKSERVER', db_user='SYSDBA', db_pass='masterkey')

#print 'servercode=TASKSERVERARTWINE'
#a = k.QKernel('ARTWINE', 'TASKSERVER')
#a = k.QKernel('DONTEXPROMSERVER', 'TASKSERVER')

#print 'servercode=PITERROBOT'
#a = k.QKernel('PITER', 'PITERROBOT')

print 'servercode=TASKSERVER'
try:
    #a = k.QKernel('PITER', 'PITERROBOT')
    #a = k.QKernel('NV', 'NVSHOP1')
    #a = k.QKernel('NVS9', 'NVSHOP9')
    #a = k.QKernel('NVS8', 'NVSHOPS8')
    #a = k.QKernel('NVS6', 'NVSHOP6')
    #a = k.QKernel('NVS4', 'NVSHOP4')
    #a = k.QKernel('statistic', 'NVSTAT')
    #a = k.QKernel('vinsent', 'TASKSERVER', 'SYSDBA', 'masterkey')
    #a = k.QKernel('MAGNIT', 'TASKSERVER')
    # a = k.QKernel('NVWH', 'TASKSERVER')
    #a = k.QKernel('NVWHTEST', 'TASKSERVER')
    #a = k.QKernel('AMSTOR', 'TASKSERVER')
    #a = k.QKernel('ARTWINE', 'TASKSERVER')
    #a = k.QKernel('URWH', 'TSD2')
    #a = k.QKernel('ZK', 'ZK')
    #a = k.QKernel('VT', 'TASKSERVER')
    #a = k.QKernel('VTSC', 'TASKSERVER', 'SYSDBA', 'masterkey')
    #a = k.QKernel('DONTEXPROM', 'TASKSERVER')
    #a = k.QKernel('SURGUT', 'TASKSERVER')
    #a = k.QKernel('UPRODTORG', 'TASKSERVER')
    a = k.QKernel('3Pine', 'TASKSERVER')
    #a = k.QKernel('BPL', 'TASKSERVER')
    #a = k.QKernel('ENGINE_LITEBOX', 'TASKSERVER')
    #a = k.QKernel('L_DR', 'TASKSERVER', 'SYSDBA', 'masterkey')
    #a = k.QKernel('L_RARUS', 'TASKSERVER', 'SYSDBA', 'masterkey')
    #a = k.QKernel('L_AKREM', 'TASKSERVER', 'SYSDBA', 'masterkey')
    #a = k.QKernel('L_VESTA', 'TASKSERVER', 'SYSDBA', 'masterkey')
    #a = k.QKernel('L_V', 'TASKSERVER', 'SYSDBA', 'masterkey')
    #a = k.QKernel('L_MIR', 'TASKSERVER', 'SYSDBA', 'masterkey', sn_name='VEGA', layer_code='OOO_Mir_=160531_110910U')
    #a = k.QKernel('Skaynet', 'TASKSERVER', 'SYSDBA', 'masterkey', sn_name='VEGA', layer_code='OOO_Skaynet=160530_092519B')
    #a = k.QKernel('L_TRIUMF', 'TASKSERVER', 'SYSDBA', 'masterkey', sn_name='VEGA', layer_code='OOO_Triumf=160614_0829186')
    # a = k.QKernel('L_GLOBAL', 'TASKSERVER', 'SYSDBA', 'masterkey')
except:
    print rqu.TracebackLog('')