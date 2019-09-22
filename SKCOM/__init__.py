import os
from comtypes.client import GetModule

HOME = os.environ["USERPROFILE"]
MODULE_NAME = os.path.join(HOME, "SKCOM.dll")

GetModule(MODULE_NAME)


class QuoteEvents:
    nKind = None
    KlineData = []

    def OnConnection(self, nKind, nCode):
        if nCode == 0:
            self.nKind = nKind
            if nKind == 3001:
                print("connecting, nkind= ", nKind)

            elif nKind == 3003:
                print("connect success, nkind= ", nKind)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        self.KlineData.append(bstrData.split(','))


class ReplyEvents:
    def OnReplyMessage(self, bstrUserID, bstrMessage):
        print('OnReplyMessage', bstrUserID, bstrMessage)
        return 0xFFFF


