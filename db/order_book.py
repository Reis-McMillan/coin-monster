import numpy as np
import voluptuous as vol
from .base import Base

def isNDArray(dims=2, dtype=np.float64):
    def validate(v):
        try:
            v = np.asarray(v, dtype=np.float64)
        except ValueError as e:
            raise vol.Invalid(str(e))
        if v.ndim != dims:
            raise vol.Invalid(f"Expected {dims}D array, got {v.ndim}D")
        if v.dtype != dtype:
            try:
                v = v.astype(dtype)
            except (ValueError, TypeError, OverflowError) as e:
                raise vol.Invalid(str(e))
        if v.shape[1] != 2:
            raise vol.Invalid('Array should have second dimension of length 2!')
        return v
    return validate
        

class OrderBook(Base):
    
    def __init__(self):
        self.name = 'order_book'
        create_cmd = f'''
            CREATE TABLE IF NOT EXISTS {self.name} (
                timestamp TIMESTAMP,
                product_id SYMBOL,
                bids DOUBLE[][],
                asks DOUBLE[][]
            ) TIMESTAMP (timestamp) 
            PARTITION BY DAY
            DEDUP UPSERT KEYS(timestamp, product_id)
        '''
        vol_schema = vol.Schema({
            vol.Required('bids'): isNDArray(),
            vol.Required('asks'): isNDArray()
        })
        super().__init__(create_cmd, vol_schema)

    def __call__(self, data: dict):
        self.insert(data)