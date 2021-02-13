# -*- coding: utf-8 -*-


import os
# 7-zip Error Codes
zipErrors={
0: '',
1: 'Warning (Non fatal error(s)). For example, one or more files were locked by some other application, so they were not compressed.',
2: 'Fatal error',
7: 'Command line error',
8: 'Not enough memory for operation',
255: 'User stopped the process',
256: 'Can not find file'
}


def Pack7z(source_file, pack_file):
    return zipErrors[os.system('7z a -y %s %s' % (pack_file, source_file))]