from copy import deepcopy
import numpy as np
from questdb.ingress import TimestampNanos
import voluptuous as vol

from .base import Base

class Ticker(Base):

    def __init__(self):
        self.name = 'ticker'
        create_cmd = f'''
            CREATE TABLE IF NOT EXISTS {self.name} (
                timestamp TIMESTAMP,
                product_id SYMBOL,
                price DOUBLE,
                volume_24_h DOUBLE,
                low_24_h DOUBLE,
                high_24_h DOUBLE,
                low_52_w DOUBLE,
                high_52_w DOUBLE,
                price_percent_chg_24_h DOUBLE,
                best_bid DOUBLE,
                best_ask DOUBLE,
                best_bid_quantity DOUBLE,
                best_ask_quantity DOUBLE
            ) TIMESTAMP (timestamp)
            PARTITION BY DAY
            DEDUP UPSERT KEYS(timestamp, product_id)
        '''
        vol_schema = vol.Schema({
            'price': vol.Coerce(float),
            'volume_24_h': vol.Coerce(float),
            'low_24_h': vol.Coerce(float),
            'high_24_h': vol.Coerce(float),
            'low_52_w': vol.Coerce(float),
            'high_52_w': vol.Coerce(float),
            'price_percent_chg_24_h': vol.Coerce(float),
            'best_bid': vol.Coerce(float),
            'best_ask': vol.Coerce(float),
            'best_bid_quantity': vol.Coerce(float),
            'best_ask_quantity': vol.Coerce(float)
        })
        super().__init__(create_cmd, vol_schema=vol_schema)

    def update(self, data):
        for trade in data['tickers']:
            trade.pop('type')
            trade['timestamp'] = TimestampNanos(int(np.datetime64(data['timestamp']).astype(int)))
            yield trade

    # a ticker update and snapshot are indiffernetiable
    # so we override Base's __call__ method and treat the two
    # event types the same
    def __call__(self, data):
        for event in data['events']:
            event_data = deepcopy(event)
            event_data['timestamp'] = data['timestamp']
            for update in self.update(event_data):
                self.insert(update)