import os

import comtypes.gen.SKCOMLib as sk
import math
from comtypes.client import CreateObject
from comtypes.client import GetModule

HOME = os.environ["USERPROFILE"]
MODULE_NAME = os.path.join(HOME, "SKCOM.dll")

GetModule(MODULE_NAME)

skQ = CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)


class QuoteEvents:
    nKind = None
    KlineData = []
    QuoteData = []

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
        quote_data = dict()
        quote_data["code"] = pStock.bstrStockNo
        quote_data["name"] = pStock.bstrStockName
        quote_data["open"] = pStock.nOpen / math.pow(10, pStock.sDecimal)
        quote_data["high"] = pStock.nHigh / math.pow(10, pStock.sDecimal)
        quote_data["low"] = pStock.nLow / math.pow(10, pStock.sDecimal)
        quote_data["match"] = pStock.nClose / math.pow(10, pStock.sDecimal)
        quote_data["volume"] = pStock.nTQty
        self.QuoteData += [quote_data]


class ReplyEvents:
    def OnReplyMessage(self, bstrUserID, bstrMessage):
        print('OnReplyMessage', bstrUserID, bstrMessage)
        return 0xFFFF


