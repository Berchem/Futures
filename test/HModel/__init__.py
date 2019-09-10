# -*- coding: utf-8 -*-
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

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

sample_matrix_ch_3 = {
    "LEVERAGE": [
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [1.0, 0.1348, 0.0768])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [1.5, 0.2073, 0.1018])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [2.0, 0.2548, 0.1055])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [2.5, 0.3005, 0.0894])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [3.0, 0.3145, 0.0757])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [3.5, 0.3311, 0.0371])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [4.0, 0.3283, -0.0129])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [4.5, 0.3063, -0.0898])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [5.0, 0.2701, -0.0935])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [5.5, 0.2269, -0.1212])),
        dict(zip(["KEY", "WeightedIndex", "FuturesPrice"], [6.0, 0.1492, -0.1305])),
    ],  # with 40 data points as interval of moving average
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