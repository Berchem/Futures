# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from Futures.Config import Config
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


# effect on difference
def estimate_effect(indicator, dst):
    leverages = [0.5 * i for i in range(2, 13)]
    intervals = [i * 10 for i in range(1, 12)]
    performance_estimation = []
    for leverage in leverages:
        for interval in intervals:
            res = futures_price.calculate(indicator=indicator, interval=interval, leverage=leverage)
            res_table = res.get()
            res_latest = res_table.rows[-1]
            reserve = res_latest["Reserve"]
            yearly_rate_of_return = (reserve / res.get("initial_reserve")) ** (1 / 20) - 1
            performance_estimation += [
                {
                    "leverage": leverage,
                    "interval": interval,
                    "roi": yearly_rate_of_return
                }
            ]
    df = pd.DataFrame(performance_estimation)
    df = df.pivot("interval", "leverage", "roi")
    fig = plt.figure(figsize=(12, 9))
    sns.heatmap(df, linewidth=0.1, cmap="YlGnBu", annot=True, fmt=".3f")
    fig.savefig(dst)


estimate_effect("volume", os.path.join(BASE_DIR, "test_resources", "HModel", "volume-roi.png"))
estimate_effect("difference", os.path.join(BASE_DIR, "test_resources", "HModel", "volume-roi.png"))
