import time
import math
import pythoncom
import asyncio
from comtypes.client import GetEvents

from . import *


class QuoteEvents:
    nKind = None
    KlineData = []
    QuoteData = []
    HistoryTicks = []
    LiveTicks = []

    def OnConnection(self, nKind, nCode):
        if nCode == 0:
            self.nKind = nKind
            if nKind == 3001:
                print("connecting, nkind= ", nKind)

            elif nKind == 3003:
                print("connect success, nkind= ", nKind)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        """
        :param bstrStockNo: item code, e.g., MTX00
        :param bstrData: history data. Open, High, Low, Close, Volume
        :return: void
        """
        self.KlineData.append([val.strip() for val in bstrData.split(',')])

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
        self.QuoteData += [quote_data]

    def OnNotifyHistoryTicks(self, sMarketNo, sStockIdx, nPtr, lDate,
                             lTimehms, lTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        self.HistoryTicks += [[sStockIdx, nPtr, lDate,
                               lTimehms, lTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate]]

    def OnNotifyTicks(self,sMarketNo, sStockIdx, nPtr, lDate,
                      lTimehms, lTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
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
        self.LiveTicks += [live_ticks_data]


class QueryUtil:
    event_quote = QuoteEvents()
    event_reply = ReplyEvents()

    handler_quote = GetEvents(skQ, event_quote)
    handler_reply = GetEvents(skR, event_reply)

    def __init__(self, conf):
        log_path = conf.prop.get("SKCOM", "LOG_PATH")
        account = conf.prop.get("SKCOM", "ID")
        password = conf.prop.get("SKCOM", "PASSWORD")

        skC.SKCenterLib_SetLogPath(log_path)
        login(account, password)

    def pump_wait(self, wait_sec=1.0, retry_limit=10, is_break=True):
        for i in range(retry_limit):
            time.sleep(wait_sec)
            pythoncom.PumpWaitingMessages()
            if self.event_quote.nKind == 3003 and is_break:
                print("retry %d times, wait %s sec" % (i + 1, (i + 1) * wait_sec))
                break

    def disconnect(self):
        try:
            return skQ.SKQuoteLib_LeaveMonitor()

        except Exception:
            raise Exception("disconnect error.")

    def get_k_line(self, item_code, k_line_type=4, output_format=1, trade_session=1):
        self.event_quote.KlineData = []
        skQ.SKQuoteLib_RequestKLineAM(item_code, k_line_type, output_format, trade_session)
        return self.event_quote.KlineData

    def get_quote(self, item_code, page=0, wait_sec=.1, retry_limit=10, is_break=False):
        skQ.SKQuoteLib_RequestStocks(page, item_code)
        self.pump_wait(wait_sec=wait_sec, retry_limit=retry_limit, is_break=is_break)
        return self.event_quote.QuoteData

    def get_history_ticks(self, item_code, page=0, wait_sec=.1, retry_limit=10, is_break=False):
        skQ.SKQuoteLib_RequestTicks(page, item_code)
        self.pump_wait(wait_sec=wait_sec, retry_limit=retry_limit, is_break=is_break)
        return self.event_quote.HistoryTicks

    def get_live_ticks(self, item_code, page=0, wait_sec=.1, retry_limit=10, is_break=False):
        skQ.SKQuoteLib_RequestLiveTick(page, item_code)
        self.pump_wait(wait_sec=wait_sec, retry_limit=retry_limit, is_break=is_break)
        return self.event_quote.LiveTicks


def connect():
    try:
        return skQ.SKQuoteLib_EnterMonitor()
    except Exception:
        raise Exception("connect error.")


