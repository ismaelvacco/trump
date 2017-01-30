import Queue
import threading
import requests
import datetime
import time
from trump.mercadobitcoin import common

def get_current_datetime():
    return datetime.datetime.now()


class Trade(object):
    """A trade event."""

    def __init__(self, dateTime, dataDict):
        self.__dateTime = dateTime
        self.__dataDict = dataDict

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getId(self):
        """Returns the trade id."""
        return self.__dataDict["tid"]

    def getTid(self):
        """Returns the trade id. (Alias from getId)"""
        return self.__dataDict["tid"]

    def getPrice(self):
        """Returns the trade price."""
        return self.__dataDict["price"]

    def getAmount(self):
        """Returns the trade amount."""
        return self.__dataDict["amount"]

    def isBuy(self):
        """Returns True if the trade was a buy."""
        return self.__dataDict["type"] == "buy"

    def isSell(self):
        """Returns True if the trade was a sell."""
        return self.__dataDict["type"] == "sell"

class OrderBookUpdate(object):
    """An order book update event."""

    def __init__(self, dateTime, eventDict):
        self.__dateTime = dateTime
        self._data = eventDict

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getBidPrices(self):
        """Returns a list with the top 20 bid prices."""
        return [float(bid[0]) for bid in self._data["bids"]]

    def getBidVolumes(self):
        """Returns a list with the top 20 bid volumes."""
        return [float(bid[1]) for bid in self._data["bids"]]

    def getAskPrices(self):
        """Returns a list with the top 20 ask prices."""
        return [float(ask[0]) for ask in self._data["asks"]]

    def getAskVolumes(self):
        """Returns a list with the top 20 ask volumes."""
        return [float(ask[1]) for ask in self._data["asks"]]

class WebServiceClient(object):

    # Events
    ON_TRADE = 1
    ON_ORDER_BOOK_UPDATE = 2
    ON_CONNECTED = 3
    ON_DISCONNECTED = 4

    MB_URL_TRADES_DATE = "https://www.mercadobitcoin.net/api/trades/%s/"
    MB_URL_TRADES = "https://www.mercadobitcoin.net/api/trades/"
    MB_URL_ORDERBOOK = "https://www.mercadobitcoin.net/api/orderbook/"
    MIN_TID = 398030

    def __init__(self):
        self.__queue = Queue.Queue()

    def getQueue(self):
        return self.__queue

    def onTrade(self, trade):
        self.__queue.put((WebServiceClient.ON_TRADE, trade))

    def onOrderBookUpdate(self, orderBookUpdate):
        self.__queue.put((WebServiceClient.ON_ORDER_BOOK_UPDATE, orderBookUpdate))

    def _getLastTid(self):
        # mush have a trade in 10 hours :-s
        lastTenHours = datetime.datetime.today() - datetime.timedelta(hours=10)
        r = requests.get(self.MB_URL_TRADES_DATE % (lastTenHours.strftime("%s")))
        lastTrades = r.json()
        return lastTrades[-1]['tid']

    def startClient(self):
        # send signal of connected
        self.__queue.put((WebServiceClient.ON_CONNECTED, None))
        tid = self._getLastTid()
        self.__alive = True
        while self.__alive:
            r = requests.get("%s?tid=%s" % (self.MB_URL_TRADES, tid))
            try:
                common.logger.debug("URL: %s" % r.url)
                for data in r.json():
                    common.logger.debug("new trade related.")
                    self.onTrade(Trade(get_current_datetime(), data))
                    if data['tid'] > tid:
                        tid = data['tid']

                r = requests.get("%s" % (self.MB_URL_ORDERBOOK))
                self.onOrderBookUpdate(OrderBookUpdate(get_current_datetime(), r.json()))
                common.logger.debug("URL: %s" % r.url)
                time.sleep(10)
            except Exception, e:
                common.logger.debug("Exception: %s" % (e))
                time.sleep(1)

    def stopClient(self):
        self.__alive = False




class WebServiceClientThread(threading.Thread):
    def __init__(self):
        super(WebServiceClientThread, self).__init__()
        self.__wsClient = WebServiceClient()

    def getQueue(self):
        return self.__wsClient.getQueue()

    def start(self):
        super(WebServiceClientThread, self).start()

    def run(self):
        self.__wsClient.startClient()

    def stop(self):
        try:
            common.logger.info("Stopping websocket client.")
            self.__wsClient.stopClient()
        except Exception, e:
            common.logger.error("Error stopping websocket client: %s." % (str(e)))
