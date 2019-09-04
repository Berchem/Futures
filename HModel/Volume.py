# -*- coding: utf-8 -*-

from Futures.Config import Config
from Futures.DataUtil import DataUtil
from Futures.Util import deprecated
from Futures.Util import MovingAverage
from Futures.Util import ClosingDates
from MypseudoSQL import Table
import time
import abc
from abc import ABC
import datetime as dt


class _VolumeIndicator(ABC):
    def __init__(self, conf):
        self._conf = conf
        # sharing config (default)
        self._CLOSING_DAY = conf.prop.get("VOLUME", "CLOSING_DAY")
        self._CLOSING_WEEK = int(conf.prop.get("VOLUME", "CLOSING_WEEK"))
        self._LEVERAGE = float(conf.prop.get("VOLUME", "LEVERAGE"))
        self._INITIAL_RESERVE = float(conf.prop.get("VOLUME", "INITIAL_RESERVE"))
        # source config
        self._INTERVAL = int(conf.prop.get("VOLUME", "INTERVAL"))
        self._DATA_RESOURCE = conf.prop.get("VOLUME", "DATA_RESOURCE")
        self._DATE_FORMAT = conf.prop.get("VOLUME", "DATE_FORMAT")
        self._COLUMN_IS_CLOSING_DATE = conf.prop.get("VOLUME", "COLUMN_IS_CLOSING_DATE")
        self._COLUMN_DATE = conf.prop.get("VOLUME", "COLUMN_DATE")
        self._COLUMN_INDEX = conf.prop.get("VOLUME", "COLUMN_INDEX")
        self._COLUMN_VOLUME = conf.prop.get("VOLUME", "COLUMN_VOLUME")
        self._COLUMN_PRICE_CURRENT = conf.prop.get("VOLUME", "COLUMN_PRICE_CURRENT")
        self._COLUMN_PRICE_NEXT = conf.prop.get("VOLUME", "COLUMN_PRICE_NEXT")
        # basic config
        self._COLUMN_AVG_INDEX = conf.prop.get("VOLUME", "COLUMN_AVG_INDEX")
        self._COLUMN_AVG_VOLUME = conf.prop.get("VOLUME", "COLUMN_AVG_VOLUME")
        self._COLUMN_VOLUME_INDICATOR = conf.prop.get("VOLUME", "COLUMN_VOLUME_INDICATOR")
        # advance config
        self._COLUMN_DELTA_OF_TARGET = conf.prop.get("VOLUME", "COLUMN_DELTA_OF_TARGET")
        self._COLUMN_INCOME_OF_TARGET = conf.prop.get("VOLUME", "COLUMN_INCOME_OF_TARGET")
        self._COLUMN_RESERVE = conf.prop.get("VOLUME", "COLUMN_RESERVE")
        self._COLUMN_OPEN_CONTRACT = conf.prop.get("VOLUME", "COLUMN_OPEN_CONTRACT")
        self._COLUMN_TRADED_CONTRACT = conf.prop.get("VOLUME", "COLUMN_TRADED_CONTRACT")

        self.__ma_obj_index = self.__init_ma_obj()
        self.__ma_obj_volume = self.__init_ma_obj()
        self.__ma_value_index = 0
        self.__ma_value_volume = 0

        self.__volume_indicator_previous = 0
        self.__volume_indicator_current = 0

        self.__value_of_target = 0
        self.__delta_of_target = 0
        self.__income_of_target = 0

        self.__reserve = 0
        self.__open_contract = 0
        self.__traded_contract = 0

        self._data = None
        self._tmp = None

    def __load_data_from_file(self, data_util):
        filename = self._conf.prop.get("VOLUME", "RESOURCE_FILENAME")
        self._data = data_util.get_data_from_file(filename, 1)

    def __load_data_from_sqlite(self, data_util):
        database = self._conf.prop.get("VOLUME", "RESOURCE_DATABASE")
        table_name = self._conf.prop.get("VOLUME", "TABLE_NAME")
        self._data = data_util.get_data_from_sqlite(database, table_name)

    def __init_ma_obj(self):
        initial_date = dt.datetime(1998, 9, 8)
        period = dt.timedelta(days=1)
        return MovingAverage(initial_date, period, self._INTERVAL)

    def __calc_ma(self, row, ma_obj, series_column_name):
        time = dt.datetime.strptime(row[self._COLUMN_DATE], self._DATE_FORMAT)
        series = float(row[series_column_name])
        ma_obj.update(time, series, series)
        _, ma_value = ma_obj.get("price")
        return ma_value

    def __check_target(self):
        return self._target in self._data.columns

    @property
    @abc.abstractmethod
    def _target(self):
        pass

    def init(self):
        if self._data is None:
            self._load_data()
            self.__check_target()

        self._tmp = self._data.select(
            additional_columns={
                # basic initialization
                self._COLUMN_AVG_INDEX: self._calc_ma_index,
                self._COLUMN_AVG_VOLUME: self._calc_ma_volume,
                self._COLUMN_VOLUME_INDICATOR: self._calc_volume_indicator,
                self._COLUMN_IS_CLOSING_DATE: self._is_closing_date,
                # advanced initialization
                self._COLUMN_DELTA_OF_TARGET.format(self._target): self._calc_delta_of_target,
                self._COLUMN_INCOME_OF_TARGET.format(self._target): self._calc_income_of_target,
            }
        )

    def _load_data(self):
        data_util = DataUtil()
        if self._DATA_RESOURCE.lower() == "csv":
            self.__load_data_from_file(data_util)

        elif self._DATA_RESOURCE.lower() == "sqlite":
            self.__load_data_from_sqlite(data_util)

        else:
            raise Exception("invalid resource")

    def _calc_ma_index(self, row):
        self.__ma_value_index = self.__calc_ma(row, self.__ma_obj_index, self._COLUMN_INDEX)
        return self.__ma_value_index

    def _calc_ma_volume(self, row):
        self.__ma_value_volume = self.__calc_ma(row, self.__ma_obj_volume, self._COLUMN_VOLUME)
        return self.__ma_value_volume

    def _is_closing_date(self, row):
        date = dt.datetime.strptime(row[self._COLUMN_DATE], self._DATE_FORMAT)
        return ClosingDates(date, self._CLOSING_DAY, self._CLOSING_WEEK).is_closing()

    def _calc_volume_indicator(self, row):
        delta = float(row[self._COLUMN_VOLUME]) - self.__ma_value_volume
        self.__volume_indicator_previous = self.__volume_indicator_current
        self.__volume_indicator_current = 1 if delta > 0 else -1 if delta < 0 else 0
        return self.__volume_indicator_current

    def _get_row_index(self, row):
        return self._data.rows.index(row)

    def _get_previous_row(self, row):
        row_index = self._get_row_index(row)
        return self._data.rows[row_index-1] if row_index > 0 else None

    @deprecated
    def _calc_delta_of_target_row_oriented(self, row):
        previous = self._get_previous_row(row)
        self.__delta_of_target = float(row[self._target]) - float(previous[self._target]) if previous else 0
        return self.__delta_of_target

    @deprecated
    def _calc_income_of_target_row_oriented(self, row):
        previous = self._get_previous_row(row)
        volume_indicator = previous[self._COLUMN_VOLUME_INDICATOR] if previous else 0
        self.__income_of_target = volume_indicator * self.__delta_of_target
        return self.__income_of_target

    def _calc_delta_of_target(self, row):
        previous_value = self.__value_of_target
        self.__value_of_target = float(row[self._target])
        self.__delta_of_target = self.__value_of_target - previous_value
        return self.__delta_of_target

    def _calc_income_of_target(self, row):
        self.__income_of_target = self.__volume_indicator_previous * self.__delta_of_target
        return self.__income_of_target

    def _calc_reserve(self, row):
        reserve = self._INITIAL_RESERVE if self.__reserve == 0 else self.__reserve
        delta_of_target = row[self._COLUMN_DELTA_OF_TARGET.format(self._target)]
        # delta_of_target = self.__delta_of_target
        open_contract = self.__open_contract
        self.__reserve = reserve + delta_of_target * open_contract
        return self.__reserve

    def _calc_traded_contract(self, row):
        previous_contract = self.__open_contract
        current_contract = self._calc_open_contract(row)
        self.__traded_contract = abs(current_contract - previous_contract)
        return self.__traded_contract

    def _calc_open_contract(self, row):
        reserve = self.__reserve
        target = float(row[self._target])
        volume_indicator = row[self._COLUMN_VOLUME_INDICATOR]
        # volume_indicator = self.__volume_indicator_current
        self.__open_contract = int(reserve * self._LEVERAGE / target) * volume_indicator
        return self.__open_contract

    def _generate_cache(self, leverage, interval, start_index):
        vi = self.__class__(self._conf)
        vi._data = self._data

        if leverage is not None:
            vi._LEVERAGE = leverage

        if interval is not None:
            vi._INTERVAL = interval

        if start_index is None:
            start_index = vi._INTERVAL - 2

        vi.init()
        vi._tmp.rows = vi._tmp.rows[start_index:]
        return vi

    def calculate(self, leverage=None, interval=None, start_index=None):
        vi = self._generate_cache(leverage, interval, start_index)
        vi._tmp = vi._tmp.select(
            additional_columns={
                vi._COLUMN_RESERVE: vi._calc_reserve,
                vi._COLUMN_TRADED_CONTRACT: vi._calc_traded_contract,
                vi._COLUMN_OPEN_CONTRACT: vi._calc_open_contract,
            }
        )
        vi._tmp.rows = vi._tmp.rows[1:]
        print("The calculation is done for leverage={}, interval={}".format(vi._LEVERAGE, vi._INTERVAL))
        return vi

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
        return self._tmp if self._tmp else self._data

    def set(self, name, value):
        setattr(self.__class__, name, value)


class FuturesPrice(_VolumeIndicator):
    def __init__(self, conf):
        _VolumeIndicator.__init__(self, conf)

    @property
    def _target(self):
        return "Price"

    def _calc_delta_of_target(self):
        column_name = "delta_of_{}".format(self._target)
        for i, row in enumerate(self._data.rows):
            if i == 0:
                row[column_name] = 0

            else:
                previous = self._data.rows[-1]
                is_close = previous[self._COLUMN_IS_CLOSING_DATE]
                previous_price = previous[self._COLUMN_PRICE_NEXT] if is_close else previous[self._COLUMN_PRICE_CURRENT]
                row[column_name] = float(row[self._COLUMN_PRICE_CURRENT]) - float(previous_price)

    def get(self):
        return self._data

    def set(self):
        pass
