import os
import json
import math
import time
import signal
import asyncio
import threading

import pythoncom
import comtypes.client
import comtypes.gen.SKCOMLib as sk

# doc 4-1 p16
skC = comtypes.client.CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)

# doc 4-4 p77
skQ = comtypes.client.CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)

# Ctrl+C 
app_done = False

class QuoteReceiver(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.ready = False
        with open('quicksk.json', 'r') as cfgfile:
            self.config = json.load(cfgfile)
            self.log_path = os.path.realpath(self.config['log_path'])

    async def wait_for_ready(self):
        while not self.ready:
            time.sleep(0.25)

    async def monitor_quote(self):
        global skC, skQ, app_done
        try:
            #skC.SKCenterLib_ResetServer('morder1.capital.com.tw')
            skC.SKCenterLib_SetLogPath(self.log_path)
            print('login', flush=True)
            retq = -1
            retc = skC.SKCenterLib_Login(self.config['account'], self.config['password'])
            if retc == 0:
                print('start monitor', flush=True)
                retry = 0
                while retq != 0 and retry < 3:
                    if retry > 0:
                        print('retry to start monitor {}'.format(retry))
                    retq = skQ.SKQuoteLib_EnterMonitor()
                    retry += 1
            else:
                msg = skC.SKCenterLib_GetReturnCodeMessage(retc)
                print('login fail: #{} {}'.format(retc, msg))

            if retq == 0:
                print('wait for monitor started')
                await self.wait_for_ready()
                print('item code', flush=True)
                list_with_comma = ','.join(self.config['products'])
                skQ.SKQuoteLib_RequestStocks(0, list_with_comma)
            else:
                print('quote unavaiable: #{}'.format(retq))

            while not app_done:
                time.sleep(1)
        except:
            print('init() unknown exception', flush=True)

    def run(self):
        ehC = comtypes.client.GetEvents(skC, self)
        ehQ = comtypes.client.GetEvents(skQ, self)
        asyncio \
            .new_event_loop() \
            .run_until_complete(self.monitor_quote())

    def OnTimer(self, nTime):
        print('OnTimer(): {}'.format(nTime), flush=True)

    def OnConnection(self, nKind, nCode):
        print('OnConnection(): nKind={}, nCode={}'.format(nKind, nCode), flush=True)
        if nKind == 3003:
            self.ready = True

    def OnNotifyQuote(self, sMarketNo, sStockidx):
        pStock = sk.SKSTOCK()
        skQ.SKQuoteLib_GetStockByIndex(sMarketNo, sStockidx, pStock)
        msg = '{} {} total volume: {} close price: {}'.format(
            pStock.bstrStockNo,
            pStock.bstrStockName,
            pStock.nTQty,
            pStock.nClose / math.pow(10, pStock.sDecimal)
        )
        print(msg)

def ctrl_c(sig, frm):
    global app_done, skQ
    print('Ctrl+C detected.')
    skQ.SKQuoteLib_LeaveMonitor()
    app_done = True

def main():
    signal.signal(signal.SIGINT, ctrl_c)
    qrcv = QuoteReceiver()
    qrcv.start()

    print('Main thread: #{}'.format(threading.get_ident()), flush=True)
    print('Receiver thread: #{}'.format(qrcv.ident), flush=True)
    while not app_done:
        pythoncom.PumpWaitingMessages()
        time.sleep(5)

if __name__ == '__main__':
    main()