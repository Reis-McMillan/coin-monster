import asyncio
import pytest
import pytest_asyncio
import asyncpg
import numpy as np
import pandas as pd
from questdb.ingress import IngressError, TimestampNanos
from voluptuous.error import MultipleInvalid
from unittest.mock import MagicMock

from config import config
from db.base import Base
from db.ticker import Ticker
from tests.fixtures.websocket import ticker as ticker_fixture

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
            await conn.execute("DROP TABLE IF EXISTS ticker")
    await Base.close()

@pytest.fixture
def sut():
    return Ticker()

@pytest.fixture
def ticker_data():
    return ticker_fixture

def sanitize(d):
    # Convert any QuestDB timestamp objects to integers for easy comparison
    return {k: (v.value if hasattr(v, 'value') else v) for k, v in d.items()}

@pytest.mark.asyncio
class TestTicker:

    async def test_create(self, pool, sut):
        await sut.create()

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT \"column\", \"type\" FROM table_columns(\'ticker\')"
            )
        result = { r["column"]: r["type"] for r in rows }
        
        expected_schema = {
            'timestamp': "TIMESTAMP",
            'product_id': "SYMBOL",
            'price': "DOUBLE",
            'volume_24_h': "DOUBLE",
            'low_24_h': "DOUBLE",
            'high_24_h': "DOUBLE",
            'low_52_w': "DOUBLE",
            'high_52_w': "DOUBLE",
            'price_percent_chg_24_h': "DOUBLE",
            'best_bid': "DOUBLE",
            'best_ask': "DOUBLE",
            'best_bid_quantity': "DOUBLE",
            'best_ask_quantity': "DOUBLE"
        }
        
        for col, col_type in expected_schema.items():
            assert result.get(col) == col_type

    test_data={
        'timestamp': TimestampNanos(1766721686000000000),
        'product_id': "BTC-USD",
        'price': 63450.50,
        'volume_24_h': 12500.25,
        'low_24_h': 62100.00,
        'high_24_h': 64800.75,
        'low_52_w': 24500.00,
        'high_52_w': 73777.00,
        'price_percent_chg_24_h': 2.15,
        'best_bid': 63450.10,
        'best_ask': 63450.90,
        'best_bid_quantity': 0.55,
        'best_ask_quantity': 1.22
    }
    @pytest.mark.parametrize("data, expected", [
        ({'missing': 60}, MultipleInvalid),
        (pd.DataFrame({"does not matter": [40]}), TypeError),
        (test_data, test_data.copy()),
        (1, TypeError)
    ])
    async def test_validate(self, sut, data, expected):
        if isinstance(expected, type) and issubclass(expected, Exception):
            with pytest.raises(expected):
                sut.validate(data)
        else:
            res = sut.validate(data)
            assert sanitize(res) == sanitize(expected)

    async def test_insert_success(self, sut, pool):
        self.test_data['product_id'] = "ETH-USD"
        sut.insert(self.test_data)
        Base.sender.flush()
        await asyncio.sleep(1)
        async with pool.acquire() as conn:
            res = await conn.fetchrow("SELECT * FROM ticker WHERE product_id = \'ETH-USD\'")
        assert res["product_id"] == "ETH-USD"
        assert res["price"] == 63450.50


    async def test_ingest_fail(self, sut):
        sut.sender = MagicMock()
        sut.sender.row.side_effect = IngressError(420, "kirked!")
        with pytest.raises(IngressError):
            sut.insert(self.test_data)
        
    async def test_call(self, sut, pool, ticker_data):
        msg = ticker_data[0]

        sut(msg)
        Base.sender.flush()
        await asyncio.sleep(1.0)

        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT * FROM ticker")
        assert len(res) > 0

    async def test_update(self, sut, ticker_data):
        msg = ticker_data[0]
        event = msg['events'][0]
        event_data = {
            'timestamp': msg['timestamp'],
            'tickers': event['tickers']
        }

        rows = list(sut.update(event_data))

        assert len(rows) == len(event['tickers'])
        for row in rows:
            assert 'timestamp' in row
            assert isinstance(row['timestamp'], TimestampNanos)
            assert 'type' not in row  # type should be popped