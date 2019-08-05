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
    :param  time: <str>, format: HHMMSSss
    :return: num: <int>
    '''
    time = time.zfill(8)
    time_list = [int(time[i:i+2]) for i in range(0, 8, 2)]
    unit_list = ["HH", "MM", "SS", "ss"]
    hash_map = dict(zip(unit_list, time_list))
    hash_map["HH"] *= 360000
    hash_map["MM"] *= 6000
    hash_map["SS"] *= 100
    return sum(val for val in hash_map.values())  # num


def num_to_time(num):
    '''
    :param   num: <int>
    :return time: <str>, format: HHMMSSss
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
    def update(self, *args):
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
        :param  time: <str> info_time
        :param price: <int> or <float> price
        :return: void
        """
        timestamp = time_to_num(time)

        if len(self.__ma_array) == 0:
            self.__ma_array.append(price)
            self.__time_array.append(time)

            if self.__timestamp is None:
                self.__timestamp = timestamp

        else:
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
        :param time : <str> info_time
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
    def __init__(self, period, initial_time):
        """
        :param period      : <int> period for refresh attributes
        :param initial_time: <str> start time, e.g. "08450000"
        """
        self.__open = None
        self.__high = None
        self.__low = None
        self.__close = None
        self.__period = period
        self.__time = initial_time
        self.__timestamp = time_to_num(initial_time)

    def update(self, time, price):
        """
        :param time : <str> info_time
        :param price: <int> or <float> price
        :return: void
        """
        timestamp = time_to_num(time)

        if self.__high is None:
            self.__high = price

        if self.__low is None:
            self.__low = price

        if self.__open is None:
            self.__open = price

        if self.__close is None:
            self.__close = price

        if timestamp < time_to_num(self.__time):
            raise Exception("timestamp is out of order")

        if timestamp < self.__timestamp + self.__period:
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
            self.__timestamp += self.__period

        self.__time = time

    def get(self):
        """
        timestamp : (start time) + n * period
        open      : opening price to current timestamp
        high      : highest price to current timestamp
        low       : lowest price to current timestamp
        close     : latest price to current timestamp
        :return: (str timestamp , int open, int, high, int low, int close)
        """
        timestamp = num_to_time(self.__timestamp)
        return timestamp, self.__open, self.__high, self.__low, self.__close


class SimpleSellBuyVolume(TechnicalIndicators):
    def __init__(self, price=None, initial_time=None):
        """
        current price  --> next price
        sell: next price < current price 內盤
        buy : next price > current price 外盤
        """
        self.__last_price = price
        self.__time = initial_time
        self.__timestamp = initial_time if initial_time is None else time_to_num(initial_time)
        self.__sell = 0
        self.__buy = 0

    def update(self, time, price, volume):
        """
        :param   time: <str> info_time
        :param  price: <int> or <float> price
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


class VolumeCount(TechnicalIndicators):
    def __init__(self, period, initial_time):
        """
        estimating the trading volume per period
        :param period      : <int> period for estimating trading volume
        :param initial_time: <str> initial time, e.g., "8450000"
        """
        self.__period = period
        self.__time = initial_time
        self.__timestamp = time_to_num(initial_time)
        self.__quantity = None
        self.__last_amount = None

    def update(self, time, amount):
        """
        :param time  : <str> time, e.g., "08450010"
        :param amount: <int> current trading volume
        :return: void
        """
        timestamp = time_to_num(time)

        if self.__quantity is None:
            self.__quantity = 0

        if self.__last_amount is None:
            self.__last_amount = amount

        if timestamp < time_to_num(self.__time):
            raise Exception("timestamp is out of order")

        if timestamp < self.__timestamp + self.__period:
            self.__quantity = amount - self.__last_amount

        else:
            self.__timestamp += self.__period
            self.__quantity = 0
            self.__last_amount = amount

    def get(self):
        """
        :return: (<str> timestamp, <int> volume in current period)
        """
        timestamp = num_to_time(self.__timestamp)
        return timestamp, self.__quantity


class AverageVolume(TechnicalIndicators):
    def __init__(self):
        self.__time = None
        self.__avg_buy = None
        self.__avg_sell = None

    def update(self, time, volume, buy_count, sell_count):
        timestamp = time_to_num(time)

        if self.__time is None:
            self.__time = time

        if timestamp < time_to_num(self.__time):
            raise Exception("timestamp is out of order")

        self.__time = time
        __volume = float(volume)
        self.__avg_buy = __volume / buy_count
        self.__avg_sell = __volume / sell_count

    def get(self):
        return self.__time, self.__avg_buy, self.__avg_sell


class SellBuy(TechnicalIndicators):
    def __init__(self):
        self.__time = None
        self.__price = None
        self.__value = None
        self.__sell_price_1 = None
        self.__buy_price_1 = None
        self.__sell_value = 0
        self.__sell_count = 0
        self.__buy_value = 0
        self.__buy_count = 0

    def __initialize_time(self, time):
        if self.__time is None:
            self.__time = time

    def __is_out_of_order(self, timestamp):
        if timestamp < time_to_num(self.__time):
            raise Exception("timestamp is out of order")

    def update(self, time, price, up1, down1, volume):
        timestamp = time_to_num(time)

        self.__initialize_time(time)
        self.__is_out_of_order(timestamp)

        self.__price = price
        self.__sell_price_1 = down1
        self.__buy_price_1 = up1
        self.__value = volume

        if self.__price < self.__sell_price_1:
            self.__sell_value += self.__value
            self.__sell_count += 1

        if self.__price > self.__buy_price_1:
            self.__buy_value += self.__value
            self.__buy_count += 1

        self.__time = time

    def __get_volume(self):
        return self.__time, self.__buy_value, self.__sell_value

    def __get_ratio(self):
        return self.__time, self.__buy_value / float(self.__sell_value + self.__buy_value)

    def __get_count(self):
        return self.__time, self.__buy_count, self.__sell_count

    def get(self, info):
        key = info.lower()
        if key == "volume":
            return self.__get_volume()

        elif key == "count":
            return self.__get_count()

        elif key == "ratio":
            return self.__get_ratio()

        else:
            raise Exception("given key: volume, count or ratio. ")


class OrderInfo(TechnicalIndicators):
    def __init__(self):
        self.__time = None
        self.__sell_volume = None
        self.__sell_count = None
        self.__buy_volume = None
        self.__buy_count = None

        self.__diff_order = None
        self.

    def update(self, time, sell_volume, sell_count, buy_volume, buy_count):
        timestamp = time_to_num(time)

        if self.__time is None:
            self.__time = time

        if timestamp < time_to_num(self.__time):
            raise Exception("timestamp is out of order")

        self.__time = time
        self.__sell_volume = float(sell_volume)
        self.__sell_count = float(sell_count)
        self.__buy_volume = float(buy_volume)
        self.__buy_count = float(buy_count)
        self.__diff_order = self.__buy_volume - self.__sell_volume

    def get(self, info):
        key = info.lower()
        if key == "diff":
            return self.__diff_order


