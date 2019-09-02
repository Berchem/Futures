# -*- coding: utf-8 -*-

from Futures.Config import Config
from Futures.DataUtil import DataUtil
from Futures.Util import MovingAverage
from Futures.Util import ClosingDates
import os
import abc
from abc import ABC
import datetime as dt


class _VolumeIndicator(ABC):
    def __init__(self, conf):
        self._conf = conf
        self._CLOSING_DAY = conf.prop.get("VOLUME", "CLOSING_DAY")
        self._CLOSING_WEEK = int(conf.prop.get("VOLUME", "CLOSING_WEEK"))
        self._COLUMN_IS_CLOSING_DATE = conf.prop.get("VOLUME", "COLUMN_IS_CLOSING_DATE")
        self._INTERVAL = int(conf.prop.get("VOLUME", "INTERVAL"))
        self._DATA_RESOURCE = conf.prop.get("VOLUME", "DATA_RESOURCE")
        self._COLUMN_DATE = conf.prop.get("VOLUME", "COLUMN_DATE")
        self._DATE_FORMAT = conf.prop.get("VOLUME", "DATE_FORMAT")
        self._COLUMN_INDEX = conf.prop.get("VOLUME", "COLUMN_INDEX")
        self._COLUMN_VOLUME = conf.prop.get("VOLUME", "COLUMN_VOLUME")
        self._COLUMN_PRICE_CURRENT = conf.prop.get("VOLUME", "COLUMN_PRICE_CURRENT")
        self._COLUMN_PRICE_NEXT = conf.prop.get("VOLUME", "COLUMN_PRICE_NEXT")
        self._COLUMN_AVG_INDEX = conf.prop.get("VOLUME", "COLUMN_AVG_INDEX")
        self._COLUMN_AVG_VOLUME = conf.prop.get("VOLUME", "COLUMN_AVG_VOLUME")
        self._COLUMN_VOLUME_INDICATOR = conf.prop.get("VOLUME", "COLUMN_VOLUME_INDICATOR")
        self._COLUMN_VOLUME_INDICATOR = conf.prop.get("VOLUME", "COLUMN_VOLUME_INDICATOR")

        self._reserve = 0
        self._open_contract = 0
        self._traded_contract = 0

        self._data = None

    def __load_data_from_file(self, data_util):
        filename = self._conf.prop.get("VOLUME", "RESOURCE_FILENAME")
        self._data = data_util.get_data_from_file(filename, 1)

    def __load_data_from_sqlite(self, data_util):
        database = self._conf.prop.get("VOLUME", "RESOURCE_DATABASE")
        table_name = self._conf.prop.get("VOLUME", "TABLE_NAME")
        self._data = data_util.get_data_from_sqlite(database, table_name)

    def __check_target(self):
        if self._target not in self._data.columns:
            raise Exception("{} not in data".format(self._target))

    @property
    @abc.abstractmethod
    def _target(self):
        pass

    def init(self):
        # basic initialization
        self._load_data()
        self._calc_ma()
        self._data = self._data.select(
            additional_columns={
                self._COLUMN_VOLUME_INDICATOR: self._calc_volume_indicator,
                self._COLUMN_IS_CLOSING_DATE: self._is_closing_date
            }
        )
        # advanced initialization
        self.__check_target()
        self._calc_delta_of_target()
        self._calc_income_of_target()

    def _load_data(self):
        data_util = DataUtil()
        if self._DATA_RESOURCE.lower() == "csv":
            self.__load_data_from_file(data_util)

        elif self._DATA_RESOURCE.lower() == "sqlite":
            self.__load_data_from_sqlite(data_util)

        else:
            raise Exception("invalid resource")

    def _calc_ma(self):
        initial_date = dt.datetime(1998, 9, 1)
        period = dt.timedelta(days=1)
        ma_index = MovingAverage(initial_date, period, self._INTERVAL)
        ma_volume = MovingAverage(initial_date, period, self._INTERVAL)

        def calc_ma(ma_obj, row, series_column_name):
            time = dt.datetime.strptime(row[self._COLUMN_DATE], self._DATE_FORMAT)
            series = float(row[series_column_name])
            ma_obj.update(time, series, series)
            _, ma_value = ma_obj.get("price")
            return ma_value

        def calc_ma_index(row):
            return calc_ma(ma_index, row, self._COLUMN_INDEX)

        def calc_ma_volume(row):
            return calc_ma(ma_volume, row, self._COLUMN_VOLUME)

        self._data = self._data.select(
            additional_columns={
                self._COLUMN_AVG_INDEX: calc_ma_index,
                self._COLUMN_AVG_VOLUME: calc_ma_volume
            }
        )

    def _is_closing_date(self, row):
        return ClosingDates(
            dt.datetime.strptime(row[self._COLUMN_DATE], self._DATE_FORMAT),
            self._CLOSING_DAY,
            self._CLOSING_WEEK
        ).is_closing()

    def _calc_volume_indicator(self, row):
        volume = float(row[self._COLUMN_VOLUME])
        avg_volume = row[self._COLUMN_AVG_VOLUME]
        return 1 if volume - avg_volume >= 0 else -1

    def _calc_delta_of_target(self):
        column_name = "delta_of_{}".format(self._target)
        for i, row in enumerate(self._data.rows):
            if i == 0:
                row[column_name] = 0

            else:
                row[column_name] = float(row[self._target]) - float(self._data.rows[i-1][self._target])

    def _calc_income_of_target(self):
        column_name = "income_of_{}".format(self._target)
        for i, row in enumerate(self._data.rows):
            volume_indicator = self._data.rows[i-1][self._COLUMN_VOLUME_INDICATOR] if i > 0 else 0
            delta_of_target = row["delta_of_{}".format(self._target)]
            row[column_name] = volume_indicator * delta_of_target

    def _calc_open_contract(self):
        pass

    def _calc_traded_contract(self):
        pass

    def _calc_reserve(self):
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

    @property
    def _target(self):
        return self._COLUMN_INDEX

    def get(self):
        return self._data

    def set(self):
        pass


class FuturesPrice(_VolumeIndicator):
    def __init__(self, conf):
        _VolumeIndicator.__init__(self, conf)

    @property
    def _target(self):
        return self._COLUMN_VOLUME

    def get(self):
        pass

    def set(self):
        pass
