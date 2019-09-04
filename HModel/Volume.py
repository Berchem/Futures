# -*- coding: utf-8 -*-

from Futures.Config import Config
from Futures.DataUtil import DataUtil
from Futures.Util import MovingAverage
import os
import abc
from abc import ABC
import datetime as dt


class _VolumeIndicator(ABC):
    def __init__(self, conf):
        self._conf = conf
        self._closing_day = conf.prop.get("VOLUME", "CLOSING_DAY")
        self._closing_week = int(conf.prop.get("VOLUME", "CLOSING_WEEK"))
        self._interval = int(conf.prop.get("VOLUME", "INTERVAL"))

        self._volume_indicator = None
        self._delta_of_target = None
        self._income_by_volume_indicator = None
        self._reserve = 0
        self._open_contract = 0
        self._traded_contract = 0

        self._data = None
        self._load_data()

    def _load_data(self):
        BASE_DIR = self._conf.prop.get("VOLUME", "BASE_DIR")
        RESOURCE_DIR = self._conf.prop.get("VOLUME", "RESOURCE_DIR")
        filename = os.path.join(BASE_DIR, RESOURCE_DIR, "history_data_for_h_model.csv")
        data_util = DataUtil()
        self._data = data_util.get_data_from_file(filename, 1)

    def _calc_ma(self):
        data = self._data
        initial_date = dt.datetime(1998, 9, 1)
        period = dt.timedelta(days=1)
        interval = self._interval
        ma_index = MovingAverage(initial_date, period, interval)
        ma_volume = MovingAverage(initial_date, period, interval)

        def calc_ma_index(row):
            date = dt.datetime.strptime(row["Date"], "%Y/%m/%d")
            index = float(row["WeightedIndex"])
            ma_index.update(date, index, index)
            _, val = ma_index.get("price")
            return val

        def calc_ma_volume(row):
            date = dt.datetime.strptime(row["Date"], "%Y/%m/%d")
            index = float(row["Volume"])
            ma_volume.update(date, index, index)
            _, val = ma_volume.get("price")
            return val

        self._data = data.select(
            additional_columns={
                "avg_index": calc_ma_index,
                "avg_volume": calc_ma_volume
            }
        )

    def _calc_volume_indicator(self):
        pass

    def _calc_delta_of_target(self):
        pass

    def _calc_income_by_volume_indicator(self):
        pass

    def _calc_reserve(self):
        pass

    def _calc_open_contract(self):
        pass

    def _calc_traded_contract(self):
        pass

    @abc.abstractmethod
    def get(self):
        pass

    @abc.abstractmethod
    def set(self):
        pass


class WeightedIndex(_VolumeIndicator):
    def __init__(self, conf):
        _VolumeIndicator.__init__(self, conf)

    def get(self):
        pass

    def set(self):
        pass


class FuturesPrice(_VolumeIndicator):
    def __init__(self, conf):
        _VolumeIndicator.__init__(self, conf)

    def get(self):
        pass

    def set(self):
        pass
