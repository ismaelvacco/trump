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

class WebServiceClient(object):

    # Events
    ON_TRADE = 1
    ON_ORDER_BOOK_UPDATE = 2
    ON_CONNECTED = 3
    ON_DISCONNECTED = 4

    MB_URL = "https://www.mercadobitcoin.net/api/trades/"
    MIN_TID = 395459

    def __init__(self):
        self.__queue = Queue.Queue()

    def getQueue(self):
        return self.__queue

    def onTrade(self, trade):
        self.__queue.put((WebServiceClient.ON_TRADE, trade))

    def startClient(self):
        tid = self.MIN_TID
        self.__alive = True
        while self.__alive:
            r = requests.get("%s?tid=%s" % (self.MB_URL, tid))
            for data in r.json():
                common.logger.info("new data in webservice.")
                self.onTrade(Trade(get_current_datetime(), data))
                if data['tid'] > tid:
                    tid = data['tid']

            common.logger.info("URL: %s" % r.url)
            time.sleep(10)

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
