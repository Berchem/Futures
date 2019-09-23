import os

import comtypes.gen.SKCOMLib as sk
import math
from comtypes.client import CreateObject
from comtypes.client import GetModule

HOME = os.environ["USERPROFILE"]
MODULE_NAME = os.path.join(HOME, "SKCOM.dll")

GetModule(MODULE_NAME)


class QuoteEvents:
    nKind = None
    KlineData = []
    QuoteData = {}

    def OnConnection(self, nKind, nCode):
        if nCode == 0:
            self.nKind = nKind
            if nKind == 3001:
                print("connecting, nkind= ", nKind)

            elif nKind == 3003:
                print("connect success, nkind= ", nKind)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        """
        :param bstrStockNo: stock no.
        :param bstrData: history data. Open, High, Low, Close, Volume
        :return: void
        """
        self.KlineData.append(bstrData.split(','))

    def OnNotifyQuote(self, sMarketNo, sStockidx):
        pStock = sk.SKSTOCK()
        skQ.SKQuoteLib_GetStockByIndex(sMarketNo, sStockidx, pStock)
        self.QuoteData["code"] = pStock.bstrStockNo
        self.QuoteData["name"] = pStock.bstrStockName
        self.QuoteData["open"] = pStock.nOpen / math.pow(10, pStock.sDecimal)
        self.QuoteData["high"] = pStock.nHigh / math.pow(10, pStock.sDecimal)
        self.QuoteData["low"] = pStock.nLow / math.pow(10, pStock.sDecimal)
        self.QuoteData["match"] = pStock.nClose / math.pow(10, pStock.sDecimal)
        self.QuoteData["volume"] = pStock.nTQty


class ReplyEvents:
    def OnReplyMessage(self, bstrUserID, bstrMessage):
        print('OnReplyMessage', bstrUserID, bstrMessage)
        return 0xFFFF


