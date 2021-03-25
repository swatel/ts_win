# -*- coding: windows-1251 -*-
﻿# coding=utf-8
__author__ = 'swat'

import print_html as Pr

text = u'''<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <title></title>
</head>
<body>
<div>Тест принтера</div>
<div>Test print</div>
</body>
</html>'''
pr = Pr.PrintHtml(text, 'name', 'd:/', margin_top=None)
if pr.convert():
    if pr.print_page():
        print pr.message
else:
    print pr.message