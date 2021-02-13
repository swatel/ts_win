# -*- coding: utf-8 -*-

import utils.file as f
import datetime as dt
import time as t
from optparse import OptionParser

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-d", "--dir", dest="directory",
                      help="TMP_DIR", metavar="DIR")
    (options, args) = parser.parse_args()
    if options.directory is None:
        parser.error('directory required')

    layer_tmp_path = options.directory
    mtime = dt.datetime.now()-dt.timedelta(days=30)
    mtime_from = t.mktime(mtime.timetuple())
    mtime = dt.datetime.now()-dt.timedelta(days=7)
    mtime_to = t.mktime(mtime.timetuple())
    tmp_files = f.check_dir_by_path(layer_tmp_path, '1', mtime_from=mtime_from, mtime_to=mtime_to)
    print(len(tmp_files))
