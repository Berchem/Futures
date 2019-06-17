from Futures.Config import Config

my_conf = Config('../conf/conf.properties')
print my_conf.conf.get('DEFAULT', 'SRC_FOLDER')
print my_conf.conf.get('CUSTOM', 'URL')
