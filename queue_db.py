# -*- coding: cp1251-*
import re
import sys

import kinterbasdb as FB

import krconst

FB.default_tpb = (
              # isc_tpb_version3 is a *purely* infrastructural value.  kinterbasdb will
              # gracefully handle user-specified TPBs that don't start with
              # isc_tpb_version3 (as well as those that do start with it).
              FB.isc_tpb_version3
            + FB.isc_tpb_write                                 # Access mode
            + FB.isc_tpb_read_committed + FB.isc_tpb_rec_version  # Isolation level
            + FB.isc_tpb_nowait                                  # Lock resolution strategy
            )
FB.init(type_conv=1, concurrency_level=2)


class QueryDB(object):
    def __init__(self, k_conf):
        self.k_conf = k_conf
        self.GetConnect()
    
    def GetConnect(self):
        self.connect = None
        self.db_message = ''
        if self.k_conf.status_config == krconst.kr_status_config_error:
            self.db_message = self.k_conf.status_config_message
        try:
            self.connect = FB.connect(dsn=self.k_conf.db_ip + ':' + self.k_conf.db_path,
                                      user=self.k_conf.db_user,
                                      password=self.k_conf.db_pass,
                                      role=self.k_conf.db_role,
                                      charset=self.k_conf.db_charset)
            # self.connect.begin()
            self.cursor = self.connect.cursor()
        except:
            #print 'self.db_message', sys.exc_value[0]
            #print 'self.db_message', sys.exc_value[1]
            #print sys.exc_value[1].find('Unable to complete network request to host')
            if sys.exc_value[0] == -902: #and sys.exc_value[1].find('Unable to complete network request to host')!=-1:
                #print 'bed connection'
                self.db_message = sys.exc_value[1]
            else:
                self.db_message = sys.exc_value[0]
        return True

    def CloseConnection(self):
        try:
            self.connect.close()
            self.connect = None
        except:
            pass

    def dbExec(self, sql, params=(), fetch='all', auto_commit=True):
        self.db_message = ''
        res = []
        if self.connect:
            try:
                self.cursor.execute(sql, params)
                # if re.findall('delete|update|execute|insert|DELETE|UPDATE|EXECUTE|INSERT', sql):
                if re.findall(r'^\s*(delete|update|execute|insert)', sql, re.IGNORECASE):
                    fetch = 'none'
                if fetch in ('many', 'all'):
                    res = self.cursor.fetchallmap()
                if fetch == 'one':
                    res = self.cursor.fetchonemap()
                if auto_commit:
                    self.connect.commit(retaining=False)
            except:
                if sys.exc_value[0] == -902 \
                        and (sys.exc_value[1]).find('Unable to complete network request to host') != -1:
                    self.db_message = sys.exc_value[1]
                else:
                    raise
                self.connect.rollback()
        else:
            self.db_message = 'Unable to complete network request to host'
        return res
    
    def commit(self, retaining=False):
        res = self.connect.commit(retaining=retaining)
        return res
    
    def savepoint(self, name):
        return self.connect.savepoint(name=name)

    def rollback(self, retaining=False, savepoint=None):
        res = self.connect.rollback(retaining=retaining, savepoint=savepoint)
        return res
