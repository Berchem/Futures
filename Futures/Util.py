import warnings
import functools
import csv


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


class MovingAverage:
    def __init__(self, interval, period, initial_time=None):
        """
        :param interval    : <int>, sequence of n values, e.g., 10
        :param period      : <int>, period for updating sequence, e.g., 6000 for 1 minute
        :param initial_time: <str>, start time, e.g., "08450000"
        """
        self.interval = interval
        self.period = period
        self.timestamp = initial_time if initial_time is None else time_to_num(initial_time)
        self.ma_value = None
        self.ma_array = []
        self.time_array = []

    def update(self, time, price):
        if len(self.ma_array) == 0:
            self.ma_array.append(price)
            self.time_array.append(time)

            if self.timestamp is None:
                self.timestamp = time_to_num(time)

        else:
            if time_to_num(time) < self.timestamp + self.period:
                self.ma_array[-1] = price
                self.time_array[-1] = time

            else:
                self.timestamp += self.period

                if len(self.ma_array) == self.interval:
                    self.ma_array = self.ma_array[1:] + [price]
                    self.time_array = self.time_array[1:] + [time]

                else:
                    self.ma_array.append(price)
                    self.time_array.append(time)

        self.ma_value = float(sum(self.ma_array)) / len(self.ma_array)

    def get(self, field=None):
        if field == "ma":
            return self.ma_value

        elif field == "time":
            return self.time_array[-1]

        else:
            return self.time_array[-1], self.ma_value
