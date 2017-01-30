from pyalgotrade import barfeed
from pyalgotrade import bar
from trump.mercadobitcoin import common
from trump.mercadobitcoin import wsclient
from pyalgotrade import observer

import datetime
import time
import Queue


class TradeBar(bar.Bar):
    # Optimization to reduce memory footprint.
    __slots__ = ('__dateTime', '__tradeId', '__price', '__amount')

    def __init__(self, dateTime, trade):
        self.__dateTime = dateTime
        self.__tradeId = trade.getId()
        self.__price = trade.getPrice()
        self.__amount = trade.getAmount()
        self.__buy = trade.isBuy()

    def __setstate__(self, state):
        (self.__dateTime, self.__tradeId, self.__price, self.__amount) = state

    def __getstate__(self):
        return (self.__dateTime, self.__tradeId, self.__price, self.__amount)

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted:
            raise Exception("Adjusted close is not available")

    def getTradeId(self):
        return self.__tradeId

    def getFrequency(self):
        return bar.Frequency.TRADE

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=False):
        return self.__price

    def getHigh(self, adjusted=False):
        return self.__price

    def getLow(self, adjusted=False):
        return self.__price

    def getClose(self, adjusted=False):
        return self.__price

    def getVolume(self):
        return self.__amount

    def getAdjClose(self):
        return None

    def getTypicalPrice(self):
        return self.__price

    def getPrice(self):
        return self.__price

    def getUseAdjValue(self):
        return False

    def isBuy(self):
        return self.__buy

    def isSell(self):
        return not self.__buy

class HistoryBarFeed(barfeed.BaseBarFeed):
    """Base class for Mercado Bitcoin based :class:`pyalgotrade.barfeed.BarFeed`.

    """
    def __init__(self, maxLen=None):
        super(BarFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__barDicts = []
        self.registerInstrument(common.btc_symbol)
        self.__prevTradeDateTime = None
        self.__initializationOk = None
        self.__stopped = False

    def __getTradeDateTime(self, trade):
        ret = trade.getDateTime()
        if ret == self.__prevTradeDateTime:
            ret += datetime.timedelta(microseconds=1)
        self.__prevTradeDateTime = ret
        return ret

    def __onTrade(self, trade):
        # Build a bar for each trade.
        barDict = {
            common.btc_symbol: TradeBar(self.__getTradeDateTime(trade), trade)
            }
        self.__barDicts.append(barDict)


class BarFeed(barfeed.BaseBarFeed):
    """Base class for Mercado Bitcoin based :class:`pyalgotrade.barfeed.BarFeed`.

    """
    QUEUE_TIMEOUT = 0.01

    def __init__(self, maxLen=None):
        super(BarFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__barDicts = []
        self.__barFilter = None
        self.registerInstrument(common.btc_symbol)
        self.__prevTradeDateTime = None
        self.__thread = None
        self.__initializationOk = None
        self.__enableReconnection = True
        self.__stopped = False
        self.__orderBookUpdateEvent = observer.Event()

    # Factory method for testing purposes.
    def buildWebServiceClientThread(self):
        return wsclient.WebServiceClientThread()

    def getCurrentDateTime(self):
        return wsclient.get_current_datetime()

    def enableReconection(self, enableReconnection):
        self.__enableReconnection = enableReconnection

    def __initializeClient(self):
        self.__initializationOk = None
        common.logger.info("Initializing websocket client.")

        try:
            # Start the thread that runs the client.
            self.__thread = self.buildWebServiceClientThread()
            self.__thread.start()
        except Exception, e:
            self.__initializationOk = False
            common.logger.error("Error connecting : %s" % str(e))

        # Wait for initialization to complete.
        while self.__initializationOk is None and self.__thread.is_alive():
            self.__dispatchImpl([wsclient.WebServiceClient.ON_CONNECTED])

        if self.__initializationOk:
            common.logger.info("Initialization ok.")
        else:
            common.logger.error("Initialization failed.")
        return self.__initializationOk

    def __onConnected(self):
        self.__initializationOk = True

    def __onDisconnected(self):
        if self.__enableReconnection:
            initialized = False
            while not self.__stopped and not initialized:
                common.logger.info("Reconnecting")
                initialized = self.__initializeClient()
                if not initialized:
                    time.sleep(5)
        else:
            self.__stopped = True

    def __dispatchImpl(self, eventFilter):
        ret = False
        try:
            eventType, eventData = self.__thread.getQueue().get(True, BarFeed.QUEUE_TIMEOUT)
            if eventFilter is not None and eventType not in eventFilter:
                common.logger.info("nothing to do")
                return False

            ret = True
            if eventType == wsclient.WebServiceClient.ON_TRADE:
                self.__onTrade(eventData)
            elif eventType == wsclient.WebServiceClient.ON_ORDER_BOOK_UPDATE:
                self.__orderBookUpdateEvent.emit(eventData)
            elif eventType == wsclient.WebServiceClient.ON_CONNECTED:
                self.__onConnected()
            elif eventType == wsclient.WebServiceClient.ON_DISCONNECTED:
                self.__onDisconnected()
            else:
                ret = False
                common.logger.error("Invalid event received to dispatch: %s - %s" % (eventType, eventData))
        except Queue.Empty:
            pass
        return ret

    # Bar datetimes should not duplicate. In case trade object datetimes conflict, we just move one slightly forward.
    def __getTradeDateTime(self, trade):
        ret = trade.getDateTime()
        if ret == self.__prevTradeDateTime:
            ret += datetime.timedelta(microseconds=1)
        self.__prevTradeDateTime = ret
        return ret

    def __onTrade(self, trade):
        # Build a bar for each trade.
        barDict = {
            common.btc_symbol: TradeBar(self.__getTradeDateTime(trade), trade)
            }
        self.__barDicts.append(barDict)

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        if len(self.__barDicts):
            ret = bar.Bars(self.__barDicts.pop(0))
        return ret

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # This may raise.
    def start(self):
        super(BarFeed, self).start()
        if self.__thread is not None:
            raise Exception("Already running")
        elif not self.__initializeClient():
            self.__stopped = True
            raise Exception("Initialization failed")

        while not self.__stopped:
            self.dispatch()
            time.sleep(3)

    def dispatch(self):
        # Note that we may return True even if we didn't dispatch any Bar
        # event.
        ret = False
        if self.__dispatchImpl(None):
            ret = True
        if super(BarFeed, self).dispatch():
            ret = True
        return ret

    # This should not raise.
    def stop(self):
        try:
            self.__stopped = True
            if self.__thread is not None and self.__thread.is_alive():
                common.logger.info("Shutting down websocket client.")
                self.__thread.stop()
        except Exception, e:
            common.logger.error("Error shutting down client: %s" % (str(e)))

    # This should not raise.
    def join(self):
        if self.__thread is not None:
            self.__thread.join()

    def eof(self):
        return self.__stopped

    def getOrderBookUpdateEvent(self):
        """
        Returns the event that will be emitted when the orderbook gets updated.

        Eventh handlers should receive one parameter:
         1. A :class:`pyalgotrade.bitstamp.wsclient.OrderBookUpdate` instance.

        :rtype: :class:`pyalgotrade.observer.Event`.
        """
        return self.__orderBookUpdateEvent
