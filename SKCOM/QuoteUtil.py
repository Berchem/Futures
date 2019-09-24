import time

import comtypes.gen.SKCOMLib as sk
import pythoncom
from comtypes.client import CreateObject
from comtypes.client import GetEvents

from SKCOM import QuoteEvents
from SKCOM import ReplyEvents


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

        except Exception:
            raise Exception("connect error.")

    def pump_wait(self, wait_sec=1, retry_limit=10, is_break=True):
        for i in range(retry_limit):
            time.sleep(wait_sec)
            pythoncom.PumpWaitingMessages()
            if self.event_quote.nKind == 3003 and is_break:
                print("retry %d times, wait %s sec" % (i + 1, (i + 1) * wait_sec))
                break

    def disconnect(self):
        try:
            return self.skQ.SKQuoteLib_LeaveMonitor()

        except Exception:
            raise Exception("disconnect error.")

    def get_k_line(self, item_code, k_line_type=4, output_format=1, trade_session=1):
        self.event_quote.KlineData = []
        self.skQ.SKQuoteLib_RequestKLineAM(item_code, k_line_type, output_format, trade_session)
        return self.event_quote.KlineData

    def get_quote(self, item_code, page=0, wait_sec=.1, retry_limit=10, is_break=False):
        self.skQ.SKQuoteLib_RequestStocks(page, item_code)
        self.pump_wait(wait_sec=wait_sec, retry_limit=retry_limit, is_break=is_break)
        return self.event_quote.QuoteData

    def get_history_ticks(self, item_code, page=0, wait_sec=.1, retry_limit=10, is_break=False):
        self.skQ.SKQuoteLib_RequestTicks(page, item_code)
        self.pump_wait(wait_sec=wait_sec, retry_limit=retry_limit, is_break=is_break)
        return self.event_quote.HistoryTicks

    def get_live_ticks(self, item_code, page=0, wait_sec=.1, retry_limit=10, is_break=False):
        self.skQ.SKQuoteLib_RequestLiveTick(page, item_code)
        self.pump_wait(wait_sec=wait_sec, retry_limit=retry_limit, is_break=is_break)
        return self.event_quote.LiveTicks




