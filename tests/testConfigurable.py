'''
Created on 2015/7/8

@author: hubo
'''
import unittest
from vlcp.config import manager, Configurable, defaultconfig
from vlcp.protocol import Protocol

@defaultconfig
class TestConfigurable(Configurable):
    _default_testproperty = '123'

@defaultconfig
class TestSubClass(TestConfigurable):
    _default_testproperty = '456'
    testproperty2 = 123

@defaultconfig
class TestShortTestConfigurable(TestConfigurable):
    _default_testproperty = '456'
    testproperty2 = 123


class Test(unittest.TestCase):

    def testConfigurable(self):
        c1 = TestConfigurable()
        c2 = TestSubClass()
        c3 = TestShortTestConfigurable()
        self.assertEqual(getattr(c1, 'test', 'notconfigured'), 'notconfigured')
        self.assertEqual(getattr(c2, 'test', 'notconfigured'), 'notconfigured')
        manager['testconfigurable.default.test'] = 789
        self.assertEqual(getattr(c1, 'test', 'notconfigured'), 789)
        self.assertEqual(getattr(c2, 'test', 'notconfigured'), 789)
        manager['testconfigurable.testsubclass.test'] = 456
        self.assertEqual(getattr(c1, 'test', 'notconfigured'), 789)
        self.assertEqual(getattr(c2, 'test', 'notconfigured'), 456)
        self.assertEqual(getattr(c1, 'testproperty', 'notconfigured'), '123')
        self.assertEqual(getattr(c2, 'testproperty', 'notconfigured'), '456')
        manager['testconfigurable.default.testproperty'] = 111
        self.assertEqual(getattr(c1, 'testproperty', 'notconfigured'), 111)
        self.assertEqual(getattr(c2, 'testproperty', 'notconfigured'), '456')
        manager['testconfigurable.testsubclass.testproperty'] = 222
        self.assertEqual(getattr(c1, 'testproperty', 'notconfigured'), 111)
        self.assertEqual(getattr(c2, 'testproperty', 'notconfigured'), 222)
        self.assertEqual(getattr(c1, 'testproperty2', 'notconfigured'), 'notconfigured')
        self.assertEqual(getattr(c2, 'testproperty2', 'notconfigured'), 123)
        manager['testconfigurable.default.testproperty2'] = 321
        self.assertEqual(getattr(c1, 'testproperty2', 'notconfigured'), 321)
        self.assertEqual(getattr(c2, 'testproperty2', 'notconfigured'), 123)
        manager['testconfigurable.testsubclass.testproperty2'] = 333
        self.assertEqual(getattr(c1, 'testproperty2', 'notconfigured'), 321)
        self.assertEqual(getattr(c2, 'testproperty2', 'notconfigured'), 123)
        c1.testproperty = 777
        c2.testproperty = 999
        self.assertEqual(getattr(c1, 'testproperty', 'notconfigured'), 777)
        self.assertEqual(getattr(c2, 'testproperty', 'notconfigured'), 999)
        self.assertEqual(getattr(c3, 'test', 'notconfigured'), 789)
        manager['testconfigurable.testshort.test'] = 888
        self.assertEqual(getattr(c3, 'test', 'notconfigured'), 888)
        self.assertEqual(manager.testconfigurable.testshort.test, 888)
        self.assertEqual(manager.testconfigurable['testshort.test'], 888)
        self.assertEqual(len(manager.testconfigurable), 3)
        self.assertEqual(set(manager.testconfigurable), set(['default', 'testsubclass', 'testshort']))
        self.assertEqual(set(manager.testconfigurable.config_keys()), set(['default.test', 'testsubclass.test', 'default.testproperty',
                                                                              'testsubclass.testproperty', 'default.testproperty2',
                                                                              'testsubclass.testproperty2', 'testshort.test']))
    def testConfigFile(self):
        manager.clear()
        manager['testa.testb.test1'] = 123
        manager['testa.testb.test2'] = "abc"
        manager['testa.testb.test3.test4'] = (123,"abc")
        manager['testa.testb.test3.test5'] = ['abc', u'def', (123,"abc")]
        manager['testa.testb.test3.test6'] = {'abc':123,b'def':u'ghi','jkl':[(123.12,345),"abc"]}
        save = manager.save()
        import os
        import tests
        os.chdir(tests.__path__[0])
        manager.saveto('../testconfigs/testconfig.cfg')
        manager.clear()
        manager.loadfrom('../testconfigs/testconfig.cfg')
        self.assertEqual(list(manager.config_items(True)), [('testa.testb.test1', 123),
                                                            ('testa.testb.test2', "abc"),
                                                            ('testa.testb.test3.test4', (123,"abc")),
                                                            ('testa.testb.test3.test5', ['abc', u'def', (123,"abc")]),
                                                            ('testa.testb.test3.test6', {'abc':123,b'def':u'ghi','jkl':[(123.12,345),"abc"]})])
        save2 = manager.save()
        self.assertEqual(save, save2)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
