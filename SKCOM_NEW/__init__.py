import os

import comtypes.gen.SKCOMLib as sk
from comtypes.client import CreateObject
from comtypes.client import GetModule

HOME = os.environ["USERPROFILE"]
MODULE_NAME = os.path.join(HOME, "SKCOM.dll")

GetModule(MODULE_NAME)

skC = CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)
skQ = CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)
skR = CreateObject(sk.SKReplyLib, interface=sk.ISKReplyLib)


class ReplyEvents:
    def OnReplyMessage(self, bstrUserID, bstrMessage):
        print('OnReplyMessage', bstrUserID, bstrMessage)
        return 0xFFFF


def login(account, password):
    try:
        return skC.SKCenterLib_Login(account, password)
    except Exception:
        raise Exception("login error.")


def result_code_mapping(result_code):
    return skC.SKCenterLib_GetReturnCodeMessage(result_code)
