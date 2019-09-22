from Futures.Config import Config
import unittest


class ConfigTest(unittest.TestCase):
    conf = Config('../test_resources/conf/conf.properties')

    def test_Config(self):
        self.assertIsNotNone(self.conf.prop)

    def test_set_property_to_local(self):
        self.conf.set_property_to_local('aaa', 'bbb', 'ccc')
        self.assertEqual('ccc', self.conf.prop.get('aaa', 'bbb'))

    def test_set_default(self):
        section = 'CUSTOM'
        # the field was not in any section, set to [DEFAULT]
        self.conf.set_default('berchem', 'lin')
        self.assertIn('berchem', self.conf.prop.options(section))
        self.assertEqual('lin', self.conf.prop.get('DEFAULT', 'berchem'))
        # the field was in a section, but field is null
        self.conf.set_default('ANY_PROPERTY', 'YEE')
        self.assertEqual('YEE', self.conf.prop.get(section, 'ANY_PROPERTY'))
        # the field and value was exist, do nothing
        self.conf.set_default('SRC_FOLDER', 'D:\\')
        self.assertNotEqual('D:\\', self.conf.prop.get(section, 'SRC_FOLDER'))

    def test_is_exist(self):
        self.assertIsNone(self.conf.is_exist('CUSTOM', 'URL'))
        self.assertRaises(Exception, self.conf.is_exist)

    def test_is_int(self):
        self.conf.set_property_to_local('aaa', 'bbb', '123')
        self.assertIsNone(self.conf.is_int('aaa', 'bbb'))
        self.assertRaises(self.conf.is_int)


if __name__ == '__main__':
    unittest.main()
