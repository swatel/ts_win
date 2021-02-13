# -*- coding: utf-8 -*-
"""
    swat 28.10.2014
    version 0.0.2.2
    модуль упаковки файлов обмена
"""

import os
import glob
import re
import time
import mx.DateTime

import krconst as k
import BasePlugin as BP
from rbsqutils import list_create_unique
from rbsqutils import pack_dir

version = '0.0.2.2'


class Plugin(BP.BasePlugin):
    """
        Класс упаковки файлов обмена
    """

    def run(self):
        tmp_dir = self.parent.k_conf.global_def_dir_tmp_files
        reg_exp = re.compile(r'Q\d+')
        arch_dir = []
        dir_list = sorted(glob.glob(tmp_dir + '/*'), key=os.path.getctime)
        ''' Текущий период для стравнений '''
        mask_dir = '%Y%m'
        if self.taskparamsxml:
            mask_dir = self.ParserXML(self.taskparamsxml, 'Mask_dir')
        cur_date_int = int(time.strftime(mask_dir, time.gmtime(mx.DateTime.now())))
        for itm_dir in dir_list:
            ''' нам нужны только папки по маске Q*'''
            q_dir = reg_exp.findall(itm_dir)
            if os.path.isdir(itm_dir) and q_dir:
                file_list = sorted(glob.glob(itm_dir + '/*.*'), key=os.path.getctime)
                for itm in file_list:
                    if not os.path.isdir(itm):
                        ''' получим год+месяц папки'''
                        dir_date_str = time.strftime(mask_dir, time.gmtime(os.path.getctime(itm)))
                        dir_date_int = int(dir_date_str)
                        if dir_date_int < cur_date_int:
                            ''' Данные папки подходят для архивирования,
                                их перемащаем в соответствующий каталог
                            '''
                            arch_dir = list_create_unique(arch_dir, tmp_dir + '/' + dir_date_str)
                            if self.move_file(itm,
                                              tmp_dir + dir_date_str + '/' + q_dir[0] + '/' + os.path.basename(itm)):
                                if self.dir_is_empty(os.path.dirname(itm)):
                                    self.delete_dir(os.path.dirname(itm))
                            else:
                                self.result['result'] = k.plugin_error
        if arch_dir:
            ''' упаковка архива '''
            for itm in arch_dir:
                if pack_dir(itm + '.7z', itm + '/*'):
                    self.delete_dir(itm + '/')
                else:
                    self.LogFile(k.m_e_pack_dir % itm)
                    self.result['result'] = k.plugin_error

    @staticmethod
    def dir_is_empty(directory):
        """
        Проверяет пуст ли каталог
        @param directory: директория
        @return: True/False
        """

        result = True
        if len(glob.glob(directory + '/*.*')) > 0:
            result = False

        return result
