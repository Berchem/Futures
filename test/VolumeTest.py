# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt

from Futures.Config import Config
from Futures.Util import clock
from HModel.Volume import *

sample_matrix = {
    "LEVERAGE": [
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [1.0, 0.113329, 0.07388])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [1.5, 0.176131, 0.100811])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [2.0, 0.196055, 0.105226])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [2.5, 0.234837, 0.092933])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [3.0, 0.234157, 0.066772])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [3.5, 0.242749, 0.028326])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [4.0, 0.219047, -0.08158])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [4.5, 0.195386, -0.09382])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [5.0, 0.151051, -0.11831])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [5.5, 0.095163, -0.12339])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [6.0, 0.016063, -0.13232])),
    ],  # with 60 data points as interval of moving average
    "INTERVAL": [
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [10, 0.1049, 0.0375])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [20, 0.0823, 0.0565])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [30, 0.1247, 0.0751])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [40, 0.1365, 0.0943])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [50, 0.1285, 0.0798])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [60, 0.1133, 0.0739])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [70, 0.0963, 0.0594])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [80, 0.0999, 0.0787])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [90, 0.0938, 0.0666])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [100, 0.0955, 0.0581])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [110, 0.0815, 0.0549])),
    ],  # with leverage = 1
}


conf = Config("../test_resources/conf/test-conf.ini")
weighted_index = WeightedIndex(conf)
weighted_index.init()
futures_price = FuturesPrice(conf)
futures_price.init()
results = []


@clock
def estimate_effect(target, factor):
    global results
    results = []
    for leverage in factor["LEVERAGE"]:
        for interval in factor["INTERVAL"]:
            res = target["obj"].calculate(leverage, interval)
            res_table = res.get()
            res_latest = res_table.rows[-1]
            reserve = res_latest["Reserve"]
            yearly_rate_of_return = (reserve / res._INITIAL_RESERVE) ** (1 / 20) - 1
            results += [{factor["main"]: leverage,
                        target["name"]: yearly_rate_of_return}]


def graph_effect(target, factor, args):
    x, book_sample, my_implement = [], [], []
    for i in range(len(results)):
        x += [sample_matrix[factor["main"]][i]["KEY"]]
        book_sample += [sample_matrix[factor["main"]][i][target["name"]] * 100]
        my_implement += [results[i][target["name"]] * 100]

    global plt
    plt.subplot(*args)
    plt.plot(x, book_sample, "o-")
    plt.plot(x, my_implement, "o-")
    plt.ylabel("Yearly Rate Of Return, %")
    plt.xlabel(factor["main"].title())
    plt.legend(["book sample", "my implement"])
    plt.title(target["name"])


def show(target, factor, args):
    estimate_effect(target, factor)
    graph_effect(target, factor, args)


def effect_of_leverage_on_weighted_index(*pos):
    target = {
        "name": "WeightedIndex",
        "obj": weighted_index,
    }

    factor = {
        "main": "LEVERAGE",
        "LEVERAGE": [i * 0.5 for i in range(2, 13)],
        "INTERVAL": [60]
    }
    show(target, factor, pos)


def effect_of_interval_on_weighted_index(*pos):
    target = {
        "name": "WeightedIndex",
        "obj": weighted_index,
    }

    factor = {
        "main": "INTERVAL",
        "LEVERAGE": [1],
        "INTERVAL": [i * 10 for i in range(1, 12)]
    }
    show(target, factor, pos)


def effect_of_leverage_on_futures_price(*pos):
    target = {
        "name": "FuturesPrice",
        "obj": futures_price,
    }

    factor = {
        "main": "LEVERAGE",
        "LEVERAGE": [i * 0.5 for i in range(2, 13)],
        "INTERVAL": [60]
    }
    show(target, factor, pos)


def effect_of_interval_on_futures_price(*pos):
    target = {
        "name": "FuturesPrice",
        "obj": futures_price,
    }

    factor = {
        "main": "INTERVAL",
        "LEVERAGE": [1],
        "INTERVAL": [i * 10 for i in range(1, 12)]
    }
    show(target, factor, pos)


# fig = plt.figure(figsize=(11.2, 9.6))
effect_of_leverage_on_weighted_index(1, 1, 1)
# effect_of_interval_on_weighted_index(2, 2, 2)
# effect_of_leverage_on_futures_price(2, 2, 3)
# effect_of_interval_on_futures_price(2, 2, 4)
# plt.tight_layout()
# fig.savefig("../test_resources/effect of leverage X interval on index X price.png")
