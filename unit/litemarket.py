# -*- coding: utf-8 -*-

import requests
import json
from orm.models.yml.Price import Price

login = 's.scheglov@litebox.ru'
password = 'litebox2015'
url = 'http://cloudretail.ru/Api/'


def post(path, xml):
    """
    Реализация метода POST
    :param path: метод (путь), добавляемый к url API
    :param xml: XML в cp1251
    :param params: dict параметров запроса (для использования в url)
    :param data: тело запроса
    :return: requests.Response
    """
    xml = xml.decode('cp1251')
    files = {
        'file': ('data.xml', xml, 'text/xml'),
        'password': (None, password),
        'login': (None, login)
    }
    return requests.post(url + path, files=files)


def post_catalog(path, xml, data={}, params=None, xml_as_param=False):
    """
    Реализация метода POST
    :param path: метод (путь), добавляемый к url API
    :param xml: XML в cp1251
    :param params: dict параметров запроса (для использования в url)
    :param data: тело запроса
    :return: requests.Response
    """
    xml = xml.decode('cp1251')
    files = {
        'loadFileModel.files[0]': ('data.xml', xml, 'text/xml'),
        'loadFileModel.format': (None, '1'),
        'password': (None, password),
        'login': (None, login)
    }
    return requests.post(url + path, files=files
)
if __name__ == "__main__":
    xml_text = """<Root>
    <GoodsOnStores>
    <GoodsOnStore article="yml_201420105526659" store="СкладCклад" count="15" goods="Стандартные булочки для гамбургеров без кунжута" />
    <GoodsOnStore article="yml_201420105526659" store="СкладCклад" count="14" goods="Стандартные булочки для гамбургеров без кунжута" />
    </GoodsOnStores>
    </Root>"""
    response = post('SetGoodsOnStores', xml_text)
    print(response.text)
    print(response.request.body)
    xml_text = """<?xml version="1.0" encoding="windows-1251"?>
 <yml_catalog date="2016-05-25 00:49">
 <shop>
 <name>      </name>
 <company>      </company>
 <currencies>
 <currency id="RUB" rate="1"/>
 </currencies>
 <categories>
 <category id="1102">                </category>
 <category parentId="1102" id="1111">            </category>
 </categories>
 <offers>
 <offer available="true" id="4025"><categoryId>1111</categoryId><name>.         Apollo-Soyuz A-18 (25  )400 (62)</name><currencyId>RUB</currencyId><price>61.0</price><description>.         Apollo-Soyuz A-18 (25  )400 (62)</description><count>10.0</count><param name="       ">1110123</param><param name="unit">  </param></offer>
 </offers>
 </shop>
 </yml_catalog>
    """
    response = post_catalog('CatalogLoadingFromXml', xml_text)
    print(response.text)
    print(response.request.body)

    print('================================ SetCostsForGoods ================================')
    xml_text = '<?xml version="1.0" encoding="UTF-8" ?><Root><GoodsCosts>'
    price = Price()
    price.name = 'Стандартные булочки для гамбургеров без кунжута'
    price.code = 'yml_201420105526659'
    price.price = 15.23
    xml_text += price.get_yml()
    xml_text += '</GoodsCosts></Root>'
    response = post('SetCostsForGoods', xml_text)
    print(response.status_code)
    print(response.text)
    print(response.request.body)
