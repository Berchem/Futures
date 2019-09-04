# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt

from Futures.Config import Config
from HModel.Volume import *

leverage_matrix = [
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [1.0, 0.113329, 0.07388])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [1.5, 0.176131, 0.100811])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [2.0, 0.196055, 0.105226])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [2.5, 0.234837, 0.092933])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [3.0, 0.234157, 0.066772])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [3.5, 0.242749, 0.028326])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [4.0, 0.219047, -0.08158])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [4.5, 0.195386, -0.09382])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [5.0, 0.151051, -0.11831])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [5.5, 0.095163, -0.12339])),
    dict(zip(["LEVERAGE", "YearlyROI", "FuturesPrice"], [6.0, 0.016063, -0.13232])),
]


@clock
def estimate_effect(obj, leverages, intervals):
    result = []
    for leverage in leverages:
        for interval in intervals:
            res = obj.calculate(leverage, interval)
            res_table = res.get()
            res_latest = res_table.rows[-1]
            reserve = res_latest["Reserve"]
            yearly_rate_of_return = (reserve / res._INITIAL_RESERVE) ** (1 / 20) - 1
            result += [{"LEVERAGE": leverage,
                        "YearlyROI": yearly_rate_of_return}]
    return result


def graph_effect(results, target):
    leverages, book_sample, my_implement = [], [], []
    for i in range(len(results)):
        leverages += [leverage_matrix[i][target]]
        book_sample += [leverage_matrix[i]["YearlyROI"] * 100]
        my_implement += [results[i]["YearlyROI"] * 100]

    plt.figure()
    plt.plot(leverages, book_sample, "o-")
    plt.plot(leverages, my_implement, "o-")
    plt.ylabel("Yearly Rate Of Return, %")
    plt.xlabel("Leverage")
    plt.legend(["book sample", "my implement"])
    plt.show()


conf = Config("../test_resources/conf/test-conf.ini")
vi_w = WeightedIndex(conf)
vi_w.init()

vi_f = FuturesPrice(conf)
vi_f.init()

results = estimate_effect(vi_w, [i * 0.5 for i in range(2, 13)], [60])
graph_effect(results, "LEVERAGE")
