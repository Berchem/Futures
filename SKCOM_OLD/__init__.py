import os

import comtypes.gen.SKCOMLib as sk
import math
from comtypes.client import CreateObject
from comtypes.client import GetModule

HOME = os.environ["USERPROFILE"]
MODULE_NAME = os.path.join(HOME, "SKCOM_OLD.dll")

GetModule(MODULE_NAME)

skQ = CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)


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


class ReplyEvents:
    def OnReplyMessage(self, bstrUserID, bstrMessage):
        print('OnReplyMessage', bstrUserID, bstrMessage)
        return 0xFFFF


