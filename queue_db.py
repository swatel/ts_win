# -*- coding: utf-8 -*-
import re
import fdb as FB

import krconst as c

FB.default_tpb = (
              # isc_tpb_version3 is a *purely* infrastructural value.  kinterbasdb will
              # gracefully handle user-specified TPBs that don't start with
              # isc_tpb_version3 (as well as those that do start with it).
              FB.isc_tpb_version3
            + FB.isc_tpb_write                                 # Access mode
            + FB.isc_tpb_read_committed + FB.isc_tpb_rec_version  # Isolation level
            + FB.isc_tpb_nowait                                  # Lock resolution strategy
            )
# FB.init(type_conv=1, concurrency_level=2)


class QueryDB(object):
    connect = None
    db_message = ''
    cursor = None

    def __init__(self, k_conf):
        self.k_conf = k_conf
        self.get_connect()

    def get_connect(self):
        self.connect = None
        self.db_message = ''
        if self.k_conf.status_config == c.kr_status_config_error:
            self.db_message = self.k_conf.status_config_message
        try:
            self.connect = FB.connect(dsn=self.k_conf.db_ip + ':' + self.k_conf.db_path,
                                      user=self.k_conf.db_user,
                                      password=self.k_conf.db_pass,
                                      role=self.k_conf.db_role,
                                      charset=self.k_conf.db_charset)
            self.cursor = self.connect.cursor()
        except Exception as exc:
            self.db_message = exc.args[0]
            return False
        return True

    def close_connect(self):
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
                if re.findall(r'^\s*(delete|update|execute|insert)', sql, re.IGNORECASE):
                    fetch = 'none'
                if fetch in ('many', 'all'):
                    res = self.cursor.fetchallmap()
                if fetch == 'one':
                    res = self.cursor.fetchonemap()
                if auto_commit:
                    self.connect.commit(retaining=False)
            except Exception as exc:
                self.db_message = exc.args[0]
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
