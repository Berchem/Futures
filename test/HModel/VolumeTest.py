# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt

from Futures.Config import Config
from Futures.Util import clock
from HModel.HModel import *
from test.HModel import *


conf = Config(os.path.join(BASE_DIR, "test_resources/conf/test-conf.ini"))
RESOURCE_FILENAME = conf.prop.get("VOLUME", "RESOURCE_FILENAME")
RESOURCE_FILENAME = os.path.join(BASE_DIR, RESOURCE_FILENAME)
conf.set_property_to_local("VOLUME", "RESOURCE_FILENAME", RESOURCE_FILENAME)

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
            res = target["obj"].calculate(leverage=leverage, interval=interval)
            res_table = res.get()
            res_latest = res_table.rows[-1]
            reserve = res_latest["Reserve"]
            yearly_rate_of_return = (reserve / res.get("initial_reserve")) ** (1 / 20) - 1
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


fig = plt.figure(figsize=(11.2, 9.6))
effect_of_leverage_on_weighted_index(2, 2, 1)
effect_of_interval_on_weighted_index(2, 2, 2)
effect_of_leverage_on_futures_price(2, 2, 3)
effect_of_interval_on_futures_price(2, 2, 4)
plt.tight_layout()

filename = os.path.join(BASE_DIR, "test_resources", "HModel",
                        "effect of leverage X interval on index X price.png")
fig.savefig(filename)
