import numpy as np
import pandas as pd
import pandera.pandas as pa
from questdb.ingress import TimestampMicros, TimestampNanos
import voluptuous as vol

from .base import Base

class Candles(Base):
    
    def __init__(self):
        self.name = 'candles'
        create_cmd = f'''
            CREATE TABLE IF NOT EXISTS {self.name} (
                timestamp TIMESTAMP,
                start TIMESTAMP,
                low DOUBLE,
                high DOUBLE,
                open DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                product_id SYMBOL   
            ) TIMESTAMP (timestamp) 
            PARTITION BY DAY
            DEDUP UPSERT KEYS(timestamp, start, product_id);
        '''
        vol_schema = vol.Schema({
            'start': TimestampMicros,
            'low': vol.Coerce(float),
            'high': vol.Coerce(float),
            'open': vol.Coerce(float),
            'close': vol.Coerce(float),
            'volume': vol.Coerce(float)
        })
        pa_schmea = pa.DataFrameSchema({
            'start': pa.Column(pa.Timestamp),
            'low': pa.Column(pa.Float64),
            'high': pa.Column(pa.Float64),
            'open': pa.Column(pa.Float64),
            'close': pa.Column(pa.Float64),
            'volume': pa.Column(pa.Float64)
        })
        super().__init__(create_cmd, vol_schema, pa_schmea)

    def snapshot(self, data):
        df = pd.DataFrame(data=data['candles'])
        df['timestamp'] = pd.Timestamp(data['timestamp'])
        df['start'] = pd.to_datetime(df['start'], unit='s')
        return df

    def update(self, data):
        for row in data['candles']:
            row['timestamp'] = TimestampNanos(int(np.datetime64(data['timestamp']).astype(int)))
            # we make `start` a TimeStampMicros object because `start` initially
            # enters the db via a pd.Dataframe where the `start` column only has
            # microsecond precision... so the db expects a TimestampMircos object
            # when we go to insert a row
            row['start'] = TimestampMicros(int(row['start']) * 1_000_000)
            yield row