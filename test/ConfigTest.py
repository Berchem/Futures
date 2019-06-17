from Futures.Config import Config

my_conf = Config('../conf/conf.properties')
print my_conf.conf.get('DEFAULT', 'SRC_FOLDER')
print my_conf.conf.get('CUSTOM', 'URL')
my_conf.set_property_to_local('aaa', 'bbb', 'ccc')
print my_conf.conf.get('aaa', 'bbb')

my_conf.is_exist('aaa', 'bbb')
# my_conf.is_exist('aaa', 'bbc')
# my_conf.is_exist('abc', 'bbb')
# my_conf.is_exist('abc', 'bbc')
my_conf.set_default('1', '2')
print my_conf.conf.get('CUSTOM', '1')
print my_conf.conf.get('aaa', '1')

print 'bbb: ', my_conf.conf.get('aaa', 'bbb')
my_conf.set_default('bbb', 'c')
print 'bbb: ', my_conf.conf.get('aaa', 'bbb')

print 'ANY_PROPERTY: ', my_conf.conf.get('CUSTOM', 'ANY_PROPERTY')
my_conf.set_default('ANY_PROPERTY', 'YEE')
# print 'ANY_PROPERTY: ', my_conf.conf.get('DEFAULT', 'ANY_PROPERTY')

my_conf.is_int('aaa', '1')
my_conf.is_int('CUSTOM', 'ANY_PROPERTY')