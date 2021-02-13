# -*- coding: utf-8 -*-

import unittest
import waresgroup as wg

VERSION = '0.0.1.1'


class WaresGroupTest(unittest.TestCase):
    """
        Тестирование WaresGroup
    """

    def setUp(self):
        self.wares_group = wg.WaresGroup(None);
        self.wares_group.waresgrid = 1
        self.wares_group.name = 'Бытовая химия'
        self.wares_group.higher = None
        self.wares_group.num = 1
        self.wares_group.code = 'CHEM'
        self.wares_group.externalcode = 'CHEMISTRY'
        self.wares_group.status = 1
        self.wares_group.externalid = '{123-456-789}'

    def test_name(self):
        self.assertEquals(self.wares_group.table_name, 'waresgroup')

    def test_getter(self):
        self.assertEquals(self.wares_group.waresgrid, 1)
        self.assertEquals(self.wares_group.name, 'Бытовая химия')
        self.assertEquals(self.wares_group.higher, None)
        self.assertEquals(self.wares_group.num, 1)
        self.assertEquals(self.wares_group.code, 'CHEM')
        self.assertEquals(self.wares_group.externalcode, 'CHEMISTRY')
        self.assertEquals(self.wares_group.status, 1)
        self.assertEquals(self.wares_group.externalid, '{123-456-789}')

    def test_setter(self):
        # Установка новых значений
        self.wares_group.waresgrid = 2
        self.wares_group.name = 'Стиральный порошок'
        self.wares_group.higher = 1
        self.wares_group.num = 1
        self.wares_group.code = 'STP'
        self.wares_group.externalcode = 'STP'
        self.wares_group.status = 1
        self.wares_group.externalid = '{123-456-789-0}'
        # Проверка
        self.assertEquals(self.wares_group.waresgrid, 2)
        self.assertEquals(self.wares_group.name, 'Стиральный порошок')
        self.assertEquals(self.wares_group.higher, 1)
        self.assertEquals(self.wares_group.num, 1)
        self.assertEquals(self.wares_group.code, 'STP')
        self.assertEquals(self.wares_group.externalcode, 'STP')
        self.assertEquals(self.wares_group.status, 1)
        self.assertEquals(self.wares_group.externalid, '{123-456-789-0}')

if __name__ == '__main__':
    unittest.main()
