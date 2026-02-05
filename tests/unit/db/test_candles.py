import asyncio
import pytest
import pytest_asyncio
import asyncpg
import numpy as np
import pandas as pd
from pandera.errors import SchemaError
from questdb.ingress import IngressError, TimestampMicros, TimestampNanos
from voluptuous.error import MultipleInvalid
from unittest.mock import MagicMock

from config import config
from db.base import Base
from db.candles import Candles
from tests.fixtures.websocket import candles as candles_fixture

@pytest_asyncio.fixture(scope="class")
async def pool():
    pool = await asyncpg.create_pool(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASS,
        database=config.DB,
        min_size=2,
        max_size=10,
        max_inactive_connection_lifetime=300,
        command_timeout=60,
    )
    Base.set_pool(pool)
    Base.establish()

    yield pool

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS candles")
    await Base.close()

@pytest.fixture
def sut():
    return Candles()

@pytest.fixture
def candles_data():
    return candles_fixture

def sanitize(d):
    # Convert any QuestDB timestamp objects to integers for easy comparison
    return {k: (v.value if hasattr(v, 'value') else v) for k, v in d.items()}

@pytest.mark.asyncio
class TestCandles:

    async def test_create(self, pool, sut):
        await sut.create()

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT \"column\", \"type\" FROM table_columns(\'candles\')"
            )
        result = { r["column"]: r["type"] for r in rows }
        
        expected_schema = {
            'timestamp': 'TIMESTAMP',
            'start': 'TIMESTAMP',
            'low': 'DOUBLE',
            'high': 'DOUBLE',
            'open': 'DOUBLE',
            'close': 'DOUBLE',
            'volume': 'DOUBLE',
            'product_id': 'SYMBOL'
        }
        
        for col, col_type in expected_schema.items():
            assert result.get(col) == col_type

    test_df = pd.DataFrame({
            'timestamp': [pd.Timestamp.now() + pd.Timedelta(minutes=5*i) for i in range(100)],
            'product_id': ['BTC-USD'] * 100,
            'start': [pd.Timestamp.now() + pd.Timedelta(minutes=i) for i in range(100)],
            'open': np.random.uniform(40000, 41000, 100),
            'high': np.random.uniform(41000, 42000, 100),
            'low': np.random.uniform(39000, 40000, 100),
            'close': np.random.uniform(40000, 41000, 100),
            'volume': np.random.uniform(0.1, 10.0, 100)
        })
    @pytest.mark.parametrize("data, expected", [
        ({"missing": 69}, MultipleInvalid), 
        ({
            "timestamp": TimestampNanos(1766721686000000000),
            "start": TimestampMicros(1766721686000000),
            "low": 408.0, "high": 420.0, "open": 410.0,
            "close": 419.0, "volume": 1.0, "product_id": "BTC-USD"
        }, {
            "timestamp": TimestampNanos(1766721686000000000),
            "start": TimestampMicros(1766721686000000),
            "low": 408.0, "high": 420.0, "open": 410.0,
            "close": 419.0, "volume": 1.0, "product_id": "BTC-USD"
        }),
        (pd.DataFrame({"missing": [68, 69]}), SchemaError),
        (test_df, test_df.copy()),
        (1, TypeError)
    ])
    async def test_validate(self, sut, data, expected):
        if isinstance(expected, type) and issubclass(expected, Exception):
            with pytest.raises(expected):
                sut.validate(data)
        else:
            res = sut.validate(data)
            if isinstance(res, dict):
                assert sanitize(res) == sanitize(expected)
            else:
                pd.testing.assert_frame_equal(res, expected)

    async def test_ingest_success(self, sut, pool):
        sut.ingest(self.test_df)
        Base.sender.flush()
        await asyncio.sleep(1.0)
        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT * FROM candles")
        res = pd.DataFrame(res, columns=list(res[0].keys()))
        res= res.reindex(sorted(res.columns), axis=1)
        expected = self.test_df.copy()
        expected = expected.reindex(sorted(expected.columns), axis=1)
        pd.testing.assert_frame_equal(res, expected)


    async def test_ingress_fail(self, sut):
        sut.sender = MagicMock()
        sut.sender.dataframe.side_effect = IngressError(420, "kirked!")
        with pytest.raises(IngressError):
            sut.ingest(self.test_df)
        
    async def test_insert_success(self, sut, pool):
        data = {
            "timestamp": TimestampNanos(1766721686000000000),
            "start": TimestampMicros(1766721686000000),
            "low": 408.0, "high": 420.0, "open": 410.0,
            "close": 419.0, "volume": 1.0, "product_id": "ETH-USD"
        }
        sut.insert(data)
        Base.sender.flush()
        await asyncio.sleep(1)
        async with pool.acquire() as conn:
            res = await conn.fetchrow("SELECT * FROM candles WHERE product_id = \'ETH-USD\'")
        assert res["product_id"] == "ETH-USD"
        assert res["high"] == 420.0

    async def test_insert_fail(self, sut):
        sut.sender = MagicMock()
        sut.sender.row.side_effect = IngressError(420, "kirked!")
        with pytest.raises(IngressError):
            data = {
                "timestamp": TimestampNanos(1766721686000000000),
                "start": TimestampMicros(1766721686000000),
                "low": 408.0, "high": 420.0, "open": 410.0,
                "close": 419.0, "volume": 1.0, "product_id": "ETH-USD"
            }
            sut.insert(data)

    async def test_call(self, sut, pool, candles_data):
        snapshot_msg = candles_data[0]  # First message is a snapshot
        update_msg = candles_data[1]    # Second message is an update

        sut(snapshot_msg)
        Base.sender.flush()
        await asyncio.sleep(1.0)

        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT * FROM candles WHERE product_id = 'BTC-USD'")
        assert len(res) > 0

        sut(update_msg)
        Base.sender.flush()
        await asyncio.sleep(1.0)

        async with pool.acquire() as conn:
            res_after_update = await conn.fetch("SELECT * FROM candles WHERE product_id = 'BTC-USD'")
        assert len(res_after_update) >= len(res)

    async def test_snapshot(self, sut, candles_data):
        snapshot_msg = candles_data[0]
        event = snapshot_msg['events'][0]
        event_data = {
            'timestamp': snapshot_msg['timestamp'],
            'candles': event['candles']
        }

        df = sut.snapshot(event_data)

        assert isinstance(df, pd.DataFrame)
        assert 'timestamp' in df.columns
        assert 'start' in df.columns
        assert 'product_id' in df.columns
        assert len(df) == len(event['candles'])

    async def test_update(self, sut, candles_data):
        update_msg = candles_data[1]
        event = update_msg['events'][0]
        event_data = {
            'timestamp': update_msg['timestamp'],
            'candles': event['candles']
        }

        rows = list(sut.update(event_data))

        assert len(rows) == len(event['candles'])
        for row in rows:
            assert 'timestamp' in row
            assert 'start' in row
            assert isinstance(row['timestamp'], TimestampNanos)
            assert isinstance(row['start'], TimestampMicros)