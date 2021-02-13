#!/bin/bash
[[ `ps -efww | grep runserver.py | grep -v grep` ]] || (/usr/local/bin/python -u ./runserver.py  >> ./log/stdout.log &)