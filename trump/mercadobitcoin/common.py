import pyalgotrade.logger
from pyalgotrade import broker


logger = pyalgotrade.logger.getLogger("mercadobitcoin")
btc_symbol = "BTC"


class BTCTraits(broker.InstrumentTraits):
    def roundQuantity(self, quantity):
        return round(quantity, 8)
