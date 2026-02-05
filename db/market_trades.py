import numpy as np
import pandas as pd
import pandera.pandas as pa
from questdb.ingress import TimestampNanos
import voluptuous as vol

from .base import Base

class MarketTrades(Base):

    def __init__(self):
        self.name = 'market_trades'
        create_cmd = f'''
            CREATE TABLE IF NOT EXISTS {self.name} (
                timestamp TIMESTAMP,
                product_id SYMBOL,
                trade_id LONG,
                price DOUBLE,
                size DOUBLE,
                side SYMBOL CAPACITY 2
            ) TIMESTAMP (timestamp)
            PARTITION BY DAY
            DEDUP UPSERT KEYS(timestamp, trade_id)
        '''
        vol_schema = vol.Schema({
            'trade_id': vol.Coerce(int),
            'price': vol.Coerce(float),
            'size': vol.Coerce(float),
            'side': vol.Coerce(str)
        })
        pa_schmea = pa.DataFrameSchema({
            'trade_id': pa.Column(pa.Int64),
            'price': pa.Column(pa.Float64),
            'size': pa.Column(pa.Float64),
            'side': pa.Column(pa.String)
        })
        super().__init__(create_cmd, vol_schema, pa_schmea, ['side'])

    def snapshot(self, data):
        df = pd.DataFrame(data=data['trades'])
        df['time'] = pd.to_datetime(df['time'])
        df.rename(columns={'time': 'timestamp'}, inplace=True)
        return df

    def update(self, data):
        for trade in data['trades']:
            trade['timestamp'] = TimestampNanos(int(np.datetime64(trade['time']).astype(int)))
            trade.pop('time')
            yield trade