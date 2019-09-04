# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt

from Futures.Config import Config
from HModel.Volume import *

sample_matrix = {
    "LEVERAGE": [
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [1.0, 0.113329, 0.07388])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [1.5, 0.176131, 0.100811])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [2.0, 0.196055, 0.105226])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [2.5, 0.234837, 0.092933])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [3.0, 0.234157, 0.066772])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [3.5, 0.242749, 0.028326])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [4.0, 0.219047, -0.08158])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [4.5, 0.195386, -0.09382])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [5.0, 0.151051, -0.11831])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [5.5, 0.095163, -0.12339])),
        dict(zip(["LEVERAGE", "WeightedIndex", "FuturesPrice"], [6.0, 0.016063, -0.13232])),
    ],  # with 60 data points as interval of moving average
    "INTERVAL": [
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [10, 0.1049, 0.0375])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [20, 0.0823, 0.0565])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [30, 0.1247, 0.0751])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [40, 0.1365, 0.0943])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [50, 0.1285, 0.0798])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [60, 0.1133, 0.0739])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [70, 0.0963, 0.0594])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [80, 0.0999, 0.0787])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [90, 0.0938, 0.0666])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [100, 0.0955, 0.0581])),
        dict(zip(["INTERVAL", "WeightedIndex", "FuturesPrice"], [110, 0.0815, 0.0549])),
    ],  # with leverage = 2
}



@clock
def estimate_leverage_effect(obj, leverages, intervals):
    result = []
    for leverage in leverages:
        for interval in intervals:
            res = obj.calculate(leverage, interval)
            res_table = res.get()
            res_latest = res_table.rows[-1]
            reserve = res_latest["Reserve"]
            yearly_rate_of_return = (reserve / res._INITIAL_RESERVE) ** (1 / 20) - 1
            result += [{"LEVERAGE": leverage,
                        "WeightedIndex": yearly_rate_of_return}]
    return result


@clock
def estimate_interval_effect(obj, leverages, intervals):
    result = []
    for leverage in leverages:
        for interval in intervals:
            res = obj.calculate(leverage, interval)
            res_table = res.get()
            res_latest = res_table.rows[-1]
            reserve = res_latest["Reserve"]
            yearly_rate_of_return = (reserve / res._INITIAL_RESERVE) ** (1 / 20) - 1
            result += [{"INTERVAL": interval,
                        "WeightedIndex": yearly_rate_of_return}]
    return result


def graph_effect(results, factor, target):
    x, book_sample, my_implement = [], [], []
    for i in range(len(results)):
        x += [sample_matrix[factor][i][factor]]
        book_sample += [sample_matrix[factor][i][target] * 100]
        my_implement += [results[i][target] * 100]

    plt.figure()
    plt.plot(x, book_sample, "o-")
    plt.plot(x, my_implement, "o-")
    plt.ylabel("Yearly Rate Of Return, %")
    plt.xlabel(factor.title())
    plt.legend(["book sample", "my implement"])
    plt.show()


conf = Config("../test_resources/conf/test-conf.ini")
vi_w = WeightedIndex(conf)
vi_w.init()

vi_f = FuturesPrice(conf)
# vi_f.init()

# results = estimate_leverage_effect(vi_w, [i * 0.5 for i in range(2, 13)], [60])
# graph_effect(results, "LEVERAGE", "WeightedIndex")

results = estimate_interval_effect(vi_w, [2], [i * 10 for i in range(1, 12)])
graph_effect(results, "INTERVAL", "WeightedIndex")
