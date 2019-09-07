# -*- coding: utf-8 -*-
from HModel import _Indicator


class WeightedIndex(_Indicator):
    def __init__(self, conf):
        _Indicator.__init__(self, conf)

    @property
    def _target(self):
        return self._COLUMN_INDEX

    def get(self, name="data"):
        attr_name = name.lower()

        if attr_name == "leverage":
            return self._LEVERAGE

        elif attr_name == "interval":
            return self._INTERVAL

        elif attr_name == "initial_reserve":
            return self._INITIAL_RESERVE

        else:
            return self._tmp if self._tmp else self._data


class FuturesPrice(_Indicator):
    def __init__(self, conf):
        _Indicator.__init__(self, conf)

        self.__value_of_target = {
            self._COLUMN_IS_CLOSING_DATE: False,
            self._COLUMN_PRICE_CURRENT: 0,
            self._COLUMN_PRICE_NEXT: 0
        }

    @property
    def _target(self):
        return "FuturesPrice"

    def _target_mapping(self):
        if self.__value_of_target[self._COLUMN_IS_CLOSING_DATE]:
            return self.__value_of_target[self._COLUMN_PRICE_NEXT]

        else:
            return self.__value_of_target[self._COLUMN_PRICE_CURRENT]

    def _calc_delta_of_target(self, row):
        previous_value = self._target_mapping()
        # update old one
        self.__value_of_target = {
            self._COLUMN_IS_CLOSING_DATE: self._is_closing,
            self._COLUMN_PRICE_CURRENT: float(row[self._COLUMN_PRICE_CURRENT]),
            self._COLUMN_PRICE_NEXT: float(row[self._COLUMN_PRICE_NEXT])
        }
        # calculate
        self._value_of_target = self.__value_of_target[self._COLUMN_PRICE_CURRENT]
        self._delta_of_target = self._value_of_target - previous_value
        return self._delta_of_target

    def _calc_open_contract(self, row):
        name = self._COLUMN_PRICE_CURRENT if row[self._COLUMN_IS_CLOSING_DATE] else self._COLUMN_PRICE_NEXT
        reserve = self._reserve
        target = float(row[name])
        volume_indicator = row[self._COLUMN_VOLUME_INDICATOR]
        self._open_contract = int(reserve * self._LEVERAGE / target) * volume_indicator
        return self._open_contract

    def get(self, name="data"):
        attr_name = name.lower()

        if attr_name == "leverage":
            return self._LEVERAGE

        elif attr_name == "interval":
            return self._INTERVAL

        elif attr_name == "initial_reserve":
            return self._INITIAL_RESERVE

        else:
            return self._tmp if self._tmp else self._data
