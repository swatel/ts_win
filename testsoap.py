# -*- coding: cp1251-*

from suds.client import Client
from suds.xsd.doctor import Import, ImportDoctor

#url = "http://127.0.0.1:8123/1cv8/ws/PhoneSpr.1cws?wsdl"
url = "http://192.168.1.93/1cv8/ws/PhoneSpr.1cws?wsdl"
user = "testtest"
paswd = "testtest"

imp = Import('http://www.w3.org/2001/XMLSchema', location='http://www.w3.org/2001/XMLSchema.xsd')
imp.filter.add('http://www.1c.ru/PhoneSpr')
doctor = ImportDoctor(imp)

print "Start"
headers = {'Content-Type': 'application/soap+xml; charset="UTF-8"'}
#client = Client(url, location=url, username=user, password=paswd, service="PhoneSpr")
client = Client(url, username=user, password=paswd)
#print client
print "Connect"
client.set_options(port='PhoneSprSoap')
print client.service.GetPhoneATS()
#result = client.messages#  service#.GetPhoneATS()
#print "result", result


'''from suds.client import Client
from suds.xsd.doctor import Import, ImportDoctor

url = "http://www.cbr.ru/dailyinfowebserv/dailyinfo.asmx?WSDL"
imp = Import('http://www.w3.org/2001/XMLSchema', location='http://www.w3.org/2001/XMLSchema.xsd')
imp.filter.add('http://web.cbr.ru/')
doctor = ImportDoctor(imp)
print "Start"
client = Client(url, doctor=doctor)
print client
print "Connect"
result = client.service.EnumValutes(True)
#print "result", result
'''