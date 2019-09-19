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

futures_price = FuturesPrice(conf)
futures_price.init()


# effect on difference
def estimate_effect(indicator, dst):
    leverages = [i for i in range(1, 11)]
    thresholds = [i / 10 for i in range(1, 11)]
    performance_estimation = []
    for leverage in leverages:
        for threshold in thresholds:
            res = futures_price.calculate(indicator=indicator, threshold=threshold, leverage=leverage)
            res_table = res.get()
            res_latest = res_table.rows[-1]
            reserve = res_latest["Reserve"]
            yearly_rate_of_return = (reserve / res.get("initial_reserve")) ** (1 / 20) - 1
            performance_estimation += [
                {
                    "leverage": leverage,
                    "threshold": threshold,
                    "roi": yearly_rate_of_return
                }
            ]
    df = pd.DataFrame(performance_estimation)
    df = df.pivot("threshold", "leverage", "roi")
    fig = plt.figure(figsize=(12, 9))
    sns.heatmap(df, linewidth=0.1, cmap="YlGnBu", annot=True, fmt=".3f")
    fig.savefig(dst)


# estimate_effect("h_model", os.path.join(BASE_DIR, "test_resources", "HModel", "h_model-roi.png"))
# res = futures_price.calculate("h_model", threshold=0.1).get("reserve")
# print(res)


init = 250000
eqv = int(init / 50)
conf.set_property_to_local("VOLUME", "INITIAL_RESERVE", str(eqv))
obj_2 = FuturesPrice(conf)
obj_2.init()
res_2 = obj_2.calculate("h_model", threshold=0.1).get("reserve")
y_roi = (res_2 / eqv) ** (1 / 20)
m_roi = y_roi ** (1 / 12)
print("init reserve:", init)
print("  yearly roi: %.2f%s" % (100 * (y_roi - 1), "%"), "\texpect:", init * (y_roi - 1))
print(" monthly roi: %.2f%s" % (100 * (m_roi - 1), "%"), "\texpect:", init * (m_roi - 1))

# conf.set_property_to_local("VOLUME", "INITIAL_RESERVE", "10000")
# obj_1 = FuturesPrice(conf)
# obj_1.init()
# res_1 = obj_1.calculate("h_model", threshold=0.1).get("reserve")

# print(res_1, "\t", (res_1 / 10000) ** (1 / 20))
# print(res_2, "\t", (res_2 / 5000) ** (1 / 20))
