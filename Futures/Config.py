import configparser


class Config:
    def __init__(self, filename):
        self.conf = configparser.ConfigParser()
        self.conf.read(filename)

