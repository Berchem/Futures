import asyncio
import datetime as dt
import threading
import time

import math
from comtypes.client import GetEvents

from Futures.Util import deprecated
from Futures.SQLiteUtil import SQLiteUtil
from . import *


def connect():
    try:
        return skQ.SKQuoteLib_EnterMonitor()
    except Exception:
        raise Exception("connect error.")


def disconnect():
    try:
        return skQ.SKQuoteLib_LeaveMonitor()
    except Exception:
        raise Exception("disconnect error.")


@deprecated
def _wait(func):
    async def wrapped(self, *args, **kwargs):
        await self._wait_for_connected()
        return await func(self, **kwargs)
    return wrapped


def wait(func):
    # async def wrapped(self, *args, **kwargs):
    async def wrapped(self, **kwargs):
        await self._wait_for_connected()
        return await func(self, **kwargs)
        # return await func(self, *args, **kwargs)
    return wrapped


# noinspection PyPep8Naming
class QueryUtil(ReplyEvents, threading.Thread):
    def __init__(self, **kwargs):
        """
        :param kwargs: {
          "account"      : <id card number>,
          "password"     : <password for capital>,
          "log_path"     : <log generated by api>,
          "process       : <quote process>,  # e.g., getKLine, getHistoryTicks, getLiveTicks & getQuote
          "item_code     : <item code>,  # e.g., MTX00, TX00 or TSEA
          "page"         : [page],  # e.g., 1, 2, default = 0
          "k_line_type"  : [period for open high low close volume],  # 0 for 1 minute, 4 for daily, etc.
          "output_format": [api output format],  # default = 0
          "trade_session": [AM or full day],  # 0 for full day, 1 for AM transaction
          "database_path": <database path>,
          "process_table": <the table for data storage>,
          "request_table": <the table for request job>,
          "multiple"     : <whether multiple threads>  # workaround for SKQuoteLib_EnterMonitor
        }
        """
        threading.Thread.__init__(self)
        ReplyEvents.__init__(self)

        self.kwargs = kwargs
        self.isConnected = False  # ret for OnConnect()
        self.gracefullyKill = False
        self.historyTicks = False  # workaround for SKQuoteLib_RequestTicks
        self.database = SQLiteUtil(kwargs["database"])
        self.multiple = kwargs["multiple"] if "multiple" in kwargs else False

        self.batch_size = 1000
        self.KLineData = []

# ============================  implement interface  ============================
    def OnConnection(self, nKind, nCode):
        if nCode == 0:
            if nKind == 3001:
                print("connecting, nkind= ", nKind)

            elif nKind == 3003:
                self.isConnected = True
                print("connected, nkind= ", nKind)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        """
        :param bstrStockNo: item code, e.g., MTX00
        :param bstrData: history data. Open, High, Low, Close, Volume
        :return: void
        """
        # item_code, datetime, open, high, low, close, volume
        self.KLineData += [[self.kwargs["item_code"]] + [val.strip() for val in bstrData.split(',')]]
        # if len(self.KLineData) > self.batch_size:
        #     self.bulk_write(self.KLineData)
        #     self.KLineData = []

    def OnNotifyQuote(self, sMarketNo, sStockidx):
        """
        :param sMarketNo: market number, e.g., 0
        :param sStockidx: item code, e.g., MTX00
        :return:
        """
        pStock = sk.SKSTOCK()
        skQ.SKQuoteLib_GetStockByIndex(sMarketNo, sStockidx, pStock)
        quote_data = dict()
        quote_data["code"] = pStock.bstrStockNo
        quote_data["name"] = pStock.bstrStockName
        quote_data["open"] = pStock.nOpen / math.pow(10, pStock.sDecimal)
        quote_data["high"] = pStock.nHigh / math.pow(10, pStock.sDecimal)
        quote_data["low"] = pStock.nLow / math.pow(10, pStock.sDecimal)
        quote_data["close"] = pStock.nClose / math.pow(10, pStock.sDecimal)
        quote_data["volume"] = pStock.nTQty
        print(quote_data)
        # self.QuoteData += [quote_data]

    def OnNotifyHistoryTicks(self, sMarketNo, sStockIdx, nPtr, lDate,
                             lTimehms, lTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        print(sStockIdx, nPtr, lDate, lTimehms, lTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate)
        self.historyTicks = True
        # self.HistoryTicks += [[sStockIdx, nPtr, lDate,
        #                        lTimehms, lTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate]]

    def OnNotifyTicks(self, sMarketNo, sStockIdx, nPtr, lDate,
                      lTimehms, lTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        if self.historyTicks:
            # SKQuoteLib_RequestTicks would callback this interface
            # while the replenishment was done
            self.gracefullyKill = True

        else:
            live_ticks_data = dict()
            live_ticks_data["sStockIdx"] = sStockIdx
            live_ticks_data["nPtr"] = nPtr
            live_ticks_data["date"] = lDate
            live_ticks_data["time"] = "%06d.%6d" % (lTimehms, lTimemillismicros)
            live_ticks_data["nBid"] = nBid
            live_ticks_data["nAsk"] = nAsk
            live_ticks_data["nClose"] = nClose
            live_ticks_data["nQty"] = nQty
            live_ticks_data["nSimulate"] = nSimulate
            print(live_ticks_data)

# ==============================  call SKCOM api  ===============================
    @wait
    async def getKLine(self, item_code, k_line_type=4, output_format=1, trade_session=1):
        skQ.SKQuoteLib_RequestKLineAM(item_code, k_line_type, output_format, trade_session)
        self.bulk_write(self.KLineData)

    @wait
    async def getHistoryTicks(self, item_code, page=0):
        skQ.SKQuoteLib_RequestTicks(page, item_code)
        self.customDaemon()

    @wait
    async def getLiveTicks(self, item_code, page=0):
        skQ.SKQuoteLib_RequestLiveTick(page, item_code)
        self.customDaemon()

    @wait
    async def getQuote(self, item_code, page=0):
        skQ.SKQuoteLib_RequestStocks(page, item_code)
        self.customDaemon()

# ==================================  methods  ==================================
    async def _wait_for_connected(self, secs=0.2):
        print("waiting for connection")
        while not self.isConnected:
            time.sleep(secs)

    def customDaemon(self):
        while not self.gracefullyKill:
            # disconnect()
            time.sleep(1)

    def bulk_write(self, value_list):
        table_name = self.kwargs["process_table"]
        columns = self.database.get_columns(table_name)
        columns.remove("index")
        self.database.bulk_insert(table_name, columns, value_list)

    def process(self, **kwargs):
        if self.kwargs["process"] == "getKLine":
            kwargs = {key: val for key, val in kwargs.items()
                      if key in ("item_code", "k_line_type", "output_format", "trade_session")}
            return self.getKLine(**kwargs)

        elif self.kwargs["process"] == "getHistoryTicks":
            kwargs = {key: val for key, val in kwargs.items()
                      if key in ("item_code", "page")}
            return self.getHistoryTicks(**kwargs)

        elif self.kwargs["process"] == "getLiveTicks":
            return self.getLiveTicks(**kwargs)

        elif self.kwargs["process"] == "getQuote":
            kwargs = {key: val for key, val in kwargs.items()
                      if key in ("item_code", "page")}
            return self.getQuote(**kwargs)

    def run(self):
        print(dt.datetime.now(), "thread {process}('{item_code}') start".format(**self.kwargs))
        event_handler_quote = GetEvents(skQ, self)
        event_handler_reply = GetEvents(skR, self)

        skC.SKCenterLib_SetLogPath(self.kwargs["log_path"])

        if not self.multiple:
            login(self.kwargs["account"], self.kwargs["password"])
            connect()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.process(**self.kwargs))

        print(dt.datetime.now(), "thread {process}('{item_code}') end".format(**self.kwargs))
