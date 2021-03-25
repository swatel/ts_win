# -*- coding: cp1251-*
import ho.pisa as pisa
from StringIO import StringIO


def create_pdf():
    out_data = {}
    out_data['html'] = open("d:/work/taskserver-source/tmp/reportQ18.html").read()
    out_data['html'] = out_data['html'].decode('utf-8').encode('cp1251')

    tmplreport = "reportmain"
    rfullfilename = "reporttest" + '.' + tmplreport
    exec('from %s import %s' % (rfullfilename, tmplreport))
    htmlreport = str(locals()[tmplreport](searchList = [out_data]))

    htmlreport = htmlreport.decode('cp1251').encode('utf-8')
    #html = "d:/test.html", "w"
    filename = "d://work//taskserver-source//tmp//reportQ18.pdf"
    #pdf = pisa.CreatePDF(htmlreport, file(filename, "wb"))
    pdf = pisa.CreatePDF(StringIO(htmlreport), file(filename, "wb"), encoding='utf-8')
    #if not pdf.err:
    #    pisa.startViewer(filename)


create_pdf()