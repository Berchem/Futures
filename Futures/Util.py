# -*- coding: utf-8 -*-
import warnings
import functools
import csv
import abc


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func


def read_csv(filename, with_header=True):
    with open(filename) as f:
        csv_reader = csv.reader(f)
        if with_header:
            next(csv_reader)
        data = [row for row in csv_reader]
    return data


def time_to_num(time):
    '''
    :param time: string, format: HHMMSSss
    :return: num: int
    '''
    time = time.zfill(8)
    time_list = [int(time[i:i+2]) for i in xrange(0, 8, 2)]
    unit_list = ["HH", "MM", "SS", "ss"]
    hash_map = dict(zip(unit_list, time_list))
    hash_map["HH"] *= 360000
    hash_map["MM"] *= 6000
    hash_map["SS"] *= 100
    return sum(val for val in hash_map.values())  # num


def num_to_time(num):
    '''
    :param num: int
    :return time: string, format: HHMMSSss
    '''
    ss = "%02d" % (num % 100)
    time = num // 100
    SS = "%02d" % (time % 60)
    time //= 60
    MM = "%02d" % (time % 60)
    time //= 60
    HH = "%02d" % (time % 60)
    return HH + MM + SS + ss


class TechnicalIndicators:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def get(self):
        pass


class MovingAverage(TechnicalIndicators):
    def __init__(self, interval, period, initial_time):
        """
        :param interval    : <int>, sequence of n values, e.g., 10
        :param period      : <int>, period for updating sequence, e.g., 6000 for 1 minute
        :param initial_time: <str>, start time, e.g., "08450000"
        """
        self.__interval = interval
        self.__period = period
        self.__timestamp = time_to_num(initial_time)
        self.__ma_value = None
        self.__ma_array = []
        self.__time_array = []

    def update(self, time, price):
        """
        :param time: <str> info_time
        :param price: <int> or <float> price
        :return: void
        """
        if len(self.__ma_array) == 0:
            self.__ma_array.append(price)
            self.__time_array.append(time)

            if self.__timestamp is None:
                self.__timestamp = time_to_num(time)

        else:
            timestamp = time_to_num(time)

            if timestamp < time_to_num(self.__time_array[-1]):
                raise Exception("timestamp is out of order")

            if timestamp < self.__timestamp + self.__period:
                self.__ma_array[-1] = price
                self.__time_array[-1] = time

            else:
                self.__timestamp += self.__period

                if len(self.__ma_array) == self.__interval:
                    self.__ma_array = self.__ma_array[1:] + [price]
                    self.__time_array = self.__time_array[1:] + [time]

                else:
                    self.__ma_array.append(price)
                    self.__time_array.append(time)

        self.__ma_value = float(sum(self.__ma_array)) / len(self.__ma_array)

    def get(self):
        """
        :return: (str raw_time, float ma_value)
        """
        return self.__time_array[-1], self.__ma_value


class HighLowPrice(TechnicalIndicators):
    def __init__(self, high=None, low=None, initial_time=None):
        """
        :param high        : <int>
        :param low         : <int>
        :param initial_time: <str>, start time, e.g., "08450000"
        """
        self.__high = high
        self.__low = low
        self.__time = initial_time
        self.__timestamp = initial_time if initial_time is None else time_to_num(initial_time)

    def update(self, time, price):
        """
        :param time: <str> info_time
        :param price: <int> or <float> price
        :return: void
        """
        timestamp = time_to_num(time)

        if self.__timestamp is None:
            self.__timestamp = timestamp

        if self.__high is None:
            self.__high = price

        if self.__low is None:
            self.__low = price

        if timestamp >= self.__timestamp:
            # if
            if price > self.__high:
                self.__high = price

            if price < self.__low:
                self.__low = price

            self.__timestamp = timestamp
            self.__time = time

        else:
            raise Exception("timestamp is out of order")

    def get(self):
        """
        :return: (str raw_time, int high, int low)
        """
        return self.__time, self.__high, self.__low


class OpenHighLowClose(TechnicalIndicators):
    def __init__(self, period):
        self.__open = None
        self.__high = None
        self.__low = None
        self.__close = None
        self.__period = period
        self.__time = None
        self.__timestamp = None

    def update(self, time, price):
        timestamp = time_to_num(time)

        if self.__timestamp is None:
            self.__timestamp = timestamp

        if self.__high is None:
            self.__high = price

        if self.__low is None:
            self.__low = price

        if self.__open is None:
            self.__open = price

        if self.__close is None:
            self.__close = price

        if timestamp >= self.__timestamp:
            timestamp_period = self.__timestamp // self.__period
            current_period = self.__timestamp // self.__period

            if current_period == timestamp_period:
                if price > self.__high:
                    self.__high = price

                if price < self.__low:
                    self.__low = price

                self.__close = price

            else:
                self.__open = price
                self.__high = price
                self.__low = price
                self.__close = price

            self.__timestamp = timestamp
            self.__time = num_to_time(current_period * self.__period)


        else:
            raise Exception("timestamp is out of order")

    def get(self):
        return self.__time, self.__open, self.__high, self.__low, self.__close


class SellBuyVolume(TechnicalIndicators):
    def __init__(self, price=None, initial_time=None):
        """
        current price  --> next price
        sell: next price < current price 內盤
        buy : next price < current price 外盤
        """
        self.__last_price = price
        self.__time = initial_time
        self.__timestamp = initial_time if initial_time is None else time_to_num(initial_time)
        self.__sell = 0
        self.__buy = 0

    def update(self, time, price, volume):
        """
        :param time: <str> info_time
        :param price: <int> or <float> price
        :param volume: <int> or <float> qty
        :return: void
        """
        timestamp = time_to_num(time)

        if self.__timestamp is None:
            self.__timestamp = timestamp

        if self.__last_price is None:
            self.__last_price = price

        if timestamp >= self.__timestamp:
            if price < self.__last_price:
                self.__sell += volume

            if price > self.__last_price:
                self.__buy += volume

            self.__last_price = price
            self.__timestamp = timestamp
            self.__time = time

        else:
            raise Exception("timestamp is out of order")

    def get(self):
        """
        :return: (<str> raw_time, <int> current_price, <int> volume of sell, <int> volume of buy)
        """
        return self.__time, self.__last_price, self.__sell, self.__buy
