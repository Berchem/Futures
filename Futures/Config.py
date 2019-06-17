from Util import deprecated
import configparser


class Config:
    def __init__(self, filename):
        self.conf = configparser.ConfigParser()
        self.__config_file = filename
        self.__get_conf_from_local()

    @deprecated
    def __get_conf(self):
        self.__get_conf_from_local()

    def __get_conf_from_local(self):
        self.conf.read(self.__config_file)

    @deprecated
    def set_property(self, section, option, value):
        self.set_property_to_local(section, option, value)

    def set_property_to_local(self, section, option, value):
        if section not in self.conf.sections() and section != 'DEFAULT':
            self.conf.add_section(section)
        self.conf.set(section, option, value)

    def set_default(self, field, default_value):
        options = [option for section in self.conf.sections() for option in self.conf.options(section)]
        if field.lower() not in options:
            self.set_property_to_local('DEFAULT', field, default_value)
        else:
            for section in self.conf.sections():
                if field.lower() in self.conf.options(section):
                    if len(self.conf.get(section, field)) == 0:
                        self.set_property_to_local(section, field, default_value)

    def is_exist(self, section, option):
        if section in self.conf.sections():
            if option not in self.conf.options(section):
                raise Exception("no Option: {} in Config file".format(option))

        else:
            raise Exception("no Section: {} in Config file".format(section))

    def is_int(self, section, option):
        value = self.conf.get(section, option)
        if not value.isdigit():
            raise Exception("{} must be a number".format(option))
