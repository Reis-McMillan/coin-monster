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
from db.level2 import Level2
from tests.fixtures.websocket import level2 as level2_fixture

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
            await conn.execute("DROP TABLE IF EXISTS level2")
    await Base.close()

@pytest.fixture
def sut():
    return Level2()

@pytest.fixture
def level2_data():
    return level2_fixture

def sanitize(d):
    # Convert any QuestDB timestamp objects to integers for easy comparison
    return {k: (v.value if hasattr(v, 'value') else v) for k, v in d.items()}

@pytest.mark.asyncio
class TestLevel2:

    async def test_create(self, pool, sut):
        await sut.create()

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT \"column\", \"type\" FROM table_columns(\'level2\')"
            )
        result = { r["column"]: r["type"] for r in rows }
        
        expected_schema = {
            'timestamp': 'TIMESTAMP',
            'product_id': 'SYMBOL',
            'side': 'SYMBOL',
            'event_time': 'TIMESTAMP',
            'price_level': 'DOUBLE',
            'new_quantity': 'DOUBLE',
        }
        
        for col, col_type in expected_schema.items():
            assert result.get(col) == col_type

    test_df = pd.DataFrame({
        'timestamp': [pd.Timestamp.now() for _ in range(100)],
        'product_id': ['BTC-USD'] * 100,
        'side': np.random.choice(['BUY', 'SELL'], 100),
        'event_time': [pd.Timestamp.now() + pd.Timedelta(seconds=np.random.randint(-100, 100)) for _ in range(100)],
        'price_level': np.random.uniform(40000, 45000, 100),
        'new_quantity': np.random.uniform(0.0, 5.0, 100)
    })
    @pytest.mark.parametrize("data, expected", [
        ({'does not matter': 67}, TypeError),
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
            res = await conn.fetch("SELECT * FROM level2")
        print(res)
        res = pd.DataFrame(res, columns=list(res[0].keys()))
        res= res.reindex(sorted(res.columns), axis=1)
        print(res)
        expected = self.test_df.copy()
        expected = expected.reindex(sorted(expected.columns), axis=1)
        expected = expected.sort_values(by='event_time').reset_index(drop=True)
        print(expected)
        pd.testing.assert_frame_equal(res, expected)


    async def test_ingest_fail(self, sut):
        sut.sender = MagicMock()
        sut.sender.dataframe.side_effect = IngressError(420, "kirked!")
        with pytest.raises(IngressError):
            sut.ingest(self.test_df)
        
    async def test_call(self, sut, pool, level2_data):
        msg = level2_data[0]

        sut(msg)
        Base.sender.flush()
        await asyncio.sleep(1.0)

        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT * FROM level2")
        assert len(res) > 0

    async def test_snapshot(self, sut, level2_data):
        msg = level2_data[0]
        event = msg['events'][0]
        event_data = {
            'timestamp': msg['timestamp'],
            'product_id': event['product_id'],
            'updates': event['updates']
        }

        df = sut.snapshot(event_data)

        assert isinstance(df, pd.DataFrame)
        assert 'timestamp' in df.columns
        assert 'product_id' in df.columns
        assert 'side' in df.columns
        assert 'event_time' in df.columns
        assert 'price_level' in df.columns
        assert 'new_quantity' in df.columns
        assert len(df) == len(event['updates'])