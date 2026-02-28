from asyncpg import Pool
from copy import deepcopy
import pandas as pd
import pandera.pandas as pa
from questdb.ingress import Sender, TimestampNanos
import voluptuous as vol


class Base():
    pool: Pool = None
    sender = Sender.from_env()
    vol_meta_schema = vol.Schema({
        'timestamp': TimestampNanos,
        'product_id': vol.Coerce(str)
    })
    pa_meta_schema = pa.DataFrameSchema({
        'timestamp': pa.Column(pa.Timestamp),
        'product_id': pa.Column(pa.String)
    })

    def __init__(self, create_cmd: str, vol_schema: vol.Schema = None,
                 pa_schema: pa.DataFrameSchema = None, symbs: list[str] = None):
        self.create_cmd = create_cmd
        if vol_schema:
            self.vol_schema = vol.Schema({**self.vol_meta_schema.schema, **vol_schema.schema})
        if pa_schema:
            self.pa_schema = pa.DataFrameSchema({**self.pa_meta_schema.columns, **pa_schema.columns}, coerce=True)
        self.symbs = ['product_id']
        if symbs:
            self.symbs.extend(symbs)
        self.timestamp = 'timestamp'

    @classmethod
    def set_pool(cls, pool: Pool):
        cls.pool = pool

    @classmethod
    def establish(cls):
        cls.sender = Sender.from_env()
        cls.sender.establish()

    @classmethod
    async def close(cls):
        await cls.pool.close()
        cls.sender.close()

    async def create(self):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(self.create_cmd)
    
    def validate(self, data: dict | pd.DataFrame):
        if isinstance(data, dict) and hasattr(self, 'vol_schema'):
            return self.vol_schema(data)
        elif isinstance(data, pd.DataFrame) and hasattr(self, 'pa_schema'):
            return self.pa_schema.validate(data)
        elif isinstance(data, dict) and not hasattr(self, 'vol_schema'):
            raise TypeError(f"{type(self).__name__} does not accept dictionaries!")
        elif isinstance(data, pd.DataFrame) and not hasattr(self, 'pa_schema'):
            raise TypeError(f"{type(self).__name__} does not accept Pandas dataframes!")
        else:
            raise TypeError("Input one of dictionary or Pandas dataframe!")

    def ingest(self, df: pd.DataFrame):
        df = self.validate(df)
        self.sender.dataframe(
                df=df,
                table_name=self.name,
                symbols=self.symbs,
                at=self.timestamp
            )

    def insert(self, data: dict):
        # keys of data that are not to be passed to columns
        data = self.validate(data)
        non_columns = self.symbs.copy()
        non_columns.append(self.timestamp)
        self.sender.row(
            table_name=self.name,
            symbols={k: data[k] for k in self.symbs},
            columns={k: data[k] for k in data.keys() if k not in non_columns},
            at=data[self.timestamp]
        )

    def __call__(self, data: dict):
        for event in data['events']:
            event_data = deepcopy(event)
            event_data['timestamp'] = data['timestamp']
            
            if event['type'] == 'snapshot':
                df = self.snapshot(event_data)
                self.ingest(df)
            
            elif event['type'] == 'update':
                for update in self.update(event_data):
                    self.insert(update)