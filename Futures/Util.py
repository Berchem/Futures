# -*- coding: utf-8 -*-
import warnings
import functools
import csv
import abc
from abc import ABC


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


class _TechnicalIndicators(ABC):
    def __init__(self):
        self._time = None

    def _is_out_of_order(self, timestamp):
        if timestamp < time_to_num(self._time):
            raise Exception("timestamp is out of order")

    @abc.abstractmethod
    def update(self, *args):
        pass

    @abc.abstractmethod
    def get(self):
        pass


class _Batched(_TechnicalIndicators, ABC):
    def __init__(self, initial_time, period):
        """
        :param initial_time: <str>, start time, e.g., "08450000"
        :param period      : <int>, period for updating sequence, e.g., 6000 for 1 minute
        """
        self._time = initial_time
        self._timestamp = time_to_num(initial_time)
        self._period = period


class _Continuous(_TechnicalIndicators, ABC):
    def _initialize_time(self, time):
        if self._time is None:
            self._time = time


class MovingAverage(_Batched):
    def __init__(self, initial_time, period, interval):
        """
        :param interval    : <int>, sequence of n values, e.g., 10
        :param period      : <int>, period for updating sequence, e.g., 6000 for 1 minute
        :param initial_time: <str>, start time, e.g., "08450000"
        """
        _Batched.__init__(self, initial_time, period)
        self.__interval = interval
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

        # throw exception
        self._is_out_of_order(timestamp)

        # updating
        if timestamp < self._timestamp + self._period:
            self.__ma_array[-1] = price
            self.__time_array[-1] = time

        else:
            self._timestamp += self._period

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


class OpenHighLowClose(_Batched):
    def __init__(self, initial_time, period):
        """
        :param period      : <int> period for refresh attributes
        :param initial_time: <str> start time, e.g. "08450000"
        """
        _Batched.__init__(self, initial_time, period)
        self.__open = None
        self.__high = None
        self.__low = None
        self.__close = None

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

        self._is_out_of_order(timestamp)

        if timestamp < self._timestamp + self._period:
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
            self._timestamp += self._period

        self._time = time

    def get(self):
        """
        timestamp : (start time) + n * period
        open      : opening price to current timestamp
        high      : highest price to current timestamp
        low       : lowest price to current timestamp
        close     : latest price to current timestamp
        :return: (str timestamp , int open, int, high, int low, int close)
        """
        time = num_to_time(self._timestamp)
        return time, self.__open, self.__high, self.__low, self.__close


class VolumeCount(_Batched):
    def __init__(self, initial_time, period):
        """
        estimating the trading volume per period
        :param initial_time: <str> initial time, e.g., "8450000"
        :param period      : <int> period for estimating trading volume
        """
        _Batched.__init__(self, initial_time, period)
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

        self._is_out_of_order(timestamp)

        if timestamp < self._timestamp + self._period:
            self.__quantity = amount - self.__last_amount

        else:
            self._timestamp += self._period
            self.__quantity = 0
            self.__last_amount = amount

    def get(self):
        """
        :return: (<str> timestamp, <int> volume in current period)
        """
        time = num_to_time(self._timestamp)
        return time, self.__quantity


class HighLowPrice(_Continuous):
    def __init__(self):
        """
        :param: void
        """
        _Continuous.__init__(self)
        self.__high = None
        self.__low = None

    def _initialize_high_low(self, price):
        """
        initilaized high & low price
        :param price: <int> or <float> price
        :return: void
        """
        if self.__high is None or self.__low is None:
            self.__high = price
            self.__low = price

    def update(self, time, price):
        """
        :param time : <str> info_time
        :param price: <int> or <float> price
        :return: void
        """
        timestamp = time_to_num(time)

        # initialized attributes
        self._initialize_time(time)
        self._initialize_high_low(price)

        # throw exception
        self._is_out_of_order(timestamp)

        # updating
        if price > self.__high:
            self.__high = price

        if price < self.__low:
            self.__low = price

        self._time = time

    def get(self):
        """
        :return: (str raw_time, int high, int low)
        """
        return self._time, self.__high, self.__low


class AverageVolume(_Continuous):
    def __init__(self):
        """
        :param: void
        """
        _Continuous.__init__(self)
        self.__avg_buy = None
        self.__avg_sell = None

    def update(self, time, volume, buy_count, sell_count):
        timestamp = time_to_num(time)

        # initialized attributes
        self._initialize_time(time)

        # throw exception
        self._is_out_of_order(timestamp)

        # updating
        __volume = float(volume)
        self.__avg_buy = __volume / buy_count
        self.__avg_sell = __volume / sell_count
        self._time = time

    def get(self):
        return self._time, self.__avg_buy, self.__avg_sell


class SimpleSellBuyVolume(_Continuous):
    def __init__(self):
        """
        current price  --> next price
        sell: next price < current price 內盤
        buy : next price > current price 外盤
        """
        _Continuous.__init__(self)
        self.__last_price = None
        self.__sell = 0
        self.__buy = 0

    def _initialize_last_price(self, price):
        """
        initialized last price
        :param price: <int> or <float> price
        :return: void
        """
        if self.__last_price is None:
            self.__last_price = price

    def update(self, time, price, volume):
        """
        :param   time: <str> info_time
        :param  price: <int> or <float> price
        :param volume: <int> or <float> qty
        :return: void
        """
        timestamp = time_to_num(time)

        # initialized attributes
        self._initialize_time(time)
        self._initialize_last_price(price)

        # throw exception
        self._is_out_of_order(timestamp)

        # updating
        if price < self.__last_price:
            self.__sell += volume

        if price > self.__last_price:
            self.__buy += volume

        self.__last_price = price
        self._time = time

    def get(self):
        """
        :return: (<str> raw_time, <int> current_price, <int> volume of sell, <int> volume of buy)
        """
        return self._time, self.__last_price, self.__sell, self.__buy


class SellBuy(_Continuous):
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


class OrderInfo(_Continuous):
    def __init__(self):
        self.__time = None
        self.__sell_volume_latest = None
        self.__sell_volume = None
        self.__sell_count = None
        self.__buy_volume_latest = None
        self.__buy_volume = None
        self.__buy_count = None

        self.__diff_sell_volume = None
        self.__diff_buy_volume = None
        self.__diff_order = None
        self.__avg_sell = None
        self.__avg_buy = None

    def update(self, time, sell_volume, sell_count, buy_volume, buy_count):
        timestamp = time_to_num(time)

        if self.__time is None:
            self.__time = time

        if self.__diff_sell_volume is None:
            self.__sell_volume_latest = float(sell_volume)

        if self.__buy_volume_latest is None:
            self.__buy_volume_latest = float(buy_volume)

        if timestamp < time_to_num(self.__time):
            raise Exception("timestamp is out of order")

        # ===== raw info =====
        self.__time = time
        self.__sell_volume = float(sell_volume)
        self.__sell_count = float(sell_count)
        self.__buy_volume = float(buy_volume)
        self.__buy_count = float(buy_count)
        # ==== indicators ====
        # difference of volume: buy - sell
        self.__diff_order = self.__buy_volume - self.__sell_volume
        # cumulative average volume: volume / count
        self.__avg_sell = self.__sell_volume / self.__sell_count
        self.__avg_buy = self.__buy_volume / self.__buy_count
        # current difference
        self.__diff_sell_volume = self.__sell_volume - self.__sell_volume_latest
        self.__diff_buy_volume = self.__buy_volume - self.__buy_volume_latest
        self.__sell_volume_latest = self.__sell_volume
        self.__buy_volume_latest = self.__buy_volume

    def get(self, info):
        key = info.lower()
        if key == "diff":
            return self.__time, self.__diff_order

        elif key == "avg":
            return self.__time, self.__avg_sell, self.__avg_buy

        elif key == "current":
            return self.__time, self.__sell_volume_latest, self.__buy_volume_latest

        else:
            raise Exception("given key: volume, count or ratio. ")
