from copy import deepcopy
import pandas as pd
import pandera.pandas as pa

from .base import Base

class Level2(Base):

    def __init__(self):
        self.name = 'level2'
        create_cmd = f'''
            CREATE TABLE IF NOT EXISTS {self.name} (
                timestamp TIMESTAMP,
                product_id SYMBOL,
                side SYMBOL CAPACITY 2,
                event_time TIMESTAMP,
                price_level DOUBLE,
                new_quantity DOUBLE
            ) TIMESTAMP (event_time)
            PARTITION BY DAY
            DEDUP UPSERT KEYS(product_id, event_time, price_level)
        '''
        pa_schema = pa.DataFrameSchema({
            'side': pa.Column(pa.String),
            'event_time': pa.Column(pa.Timestamp),
            'price_level': pa.Column(pa.Float64),
            'new_quantity': pa.Column(pa.Float64)
        })
        super().__init__(create_cmd, pa_schema=pa_schema , symbs=['side'])
        self.timestamp = 'event_time'

    def snapshot(self, data):
        df = pd.DataFrame(data=data['updates'])
        df['timestamp'] = pd.Timestamp(data['timestamp'])
        df['product_id'] = data['product_id']
        df['event_time'] = pd.to_datetime(df['event_time'])
        return df

    # a level2 update and snapshot are indiffernetiable
    # so we override Base's __call__ method and treat the two
    # event types the same... kinda like ticker
    def __call__(self, data):
        for event in data['events']:
            event_data = deepcopy(event)
            event_data['timestamp'] = data['timestamp']
            df = self.snapshot(event_data)
            self.ingest(df)