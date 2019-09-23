import time

import comtypes.gen.SKCOMLib as sk
import math
import pythoncom
from comtypes.client import CreateObject
from comtypes.client import GetEvents

from SKCOM import QuoteEvents
from SKCOM import ReplyEvents


# skQ = CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)


# class QuoteEvents:
#     nKind = None
#     KlineData = []
#     QuoteData = []
#
#     def OnConnection(self, nKind, nCode):
#         if nCode == 0:
#             self.nKind = nKind
#             if nKind == 3001:
#                 print("connecting, nkind= ", nKind)
#
#             elif nKind == 3003:
#                 print("connect success, nkind= ", nKind)
#
#     def OnNotifyKLineData(self, bstrStockNo, bstrData):
#         """
#         :param bstrStockNo: stock no.
#         :param bstrData: history data. Open, High, Low, Close, Volume
#         :return: void
#         """
#         self.KlineData.append(bstrData.split(','))
#
#     def OnNotifyQuote(self, sMarketNo, sStockidx):
#         pStock = sk.SKSTOCK()
#         skQ.SKQuoteLib_GetStockByIndex(sMarketNo, sStockidx, pStock)
#         quote_data = dict()
#         quote_data["code"] = pStock.bstrStockNo
#         quote_data["name"] = pStock.bstrStockName
#         quote_data["open"] = pStock.nOpen / math.pow(10, pStock.sDecimal)
#         quote_data["high"] = pStock.nHigh / math.pow(10, pStock.sDecimal)
#         quote_data["low"] = pStock.nLow / math.pow(10, pStock.sDecimal)
#         quote_data["match"] = pStock.nClose / math.pow(10, pStock.sDecimal)
#         quote_data["volume"] = pStock.nTQty
#         self.QuoteData += [quote_data]


class QueryUtil:
    skC = CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)
    skQ = CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)
    skR = CreateObject(sk.SKReplyLib, interface=sk.ISKReplyLib)

    event_quote = QuoteEvents()
    event_reply = ReplyEvents()

    event_handler_quote = GetEvents(skQ, event_quote)
    conn_reply = GetEvents(skR, event_reply)

    def __init__(self, conf):
        log_path = conf.prop.get("SKCOM", "LOG_PATH")
        account = conf.prop.get("SKCOM", "ID")
        password = conf.prop.get("SKCOM", "PASSWORD")

        self.skC.SKCenterLib_SetLogPath(log_path)
        self.login(account, password)

    def result_code_mapping(self, result_code):
        return self.skC.SKCenterLib_GetReturnCodeMessage(result_code)

    def login(self, account, password):
        try:
            return self.skC.SKCenterLib_Login(account, password)

        except Exception:
            raise Exception("login error.")

    def connect(self):
        try:
            return self.skQ.SKQuoteLib_EnterMonitor()
            # return skQ.SKQuoteLib_EnterMonitor()

        except Exception:
            raise Exception("connect error.")

    def pump_wait(self, retry_limit=10):
        for _ in range(retry_limit):
            time.sleep(1)
            pythoncom.PumpWaitingMessages()
            if self.event_quote.nKind == 3003:
                break

    def disconnect(self):
        try:
            return self.skQ.SKQuoteLib_LeaveMonitor()

        except Exception:
            raise Exception("disconnect error.")

    def get_k_line(self, item_code, k_line_type=4, output_format=1, trade_session=1):
        self.event_quote.KlineData = []
        self.skQ.SKQuoteLib_RequestKLineAM(item_code, k_line_type, output_format, trade_session)
        # skQ.SKQuoteLib_RequestKLineAM(item_code, k_line_type, output_format, trade_session)
        return self.event_quote.KlineData

    def get_quote(self, item_code, page=0):
        self.skQ.SKQuoteLib_RequestStocks(page, item_code)
        # skQ.SKQuoteLib_RequestStocks(page, item_code)
        return self.event_quote.QuoteData



