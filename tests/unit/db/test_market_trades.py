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
from db.market_trades import MarketTrades
from tests.fixtures.websocket import market_trades as market_trades_fixture

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
            await conn.execute("DROP TABLE IF EXISTS market_trades")
    await Base.close()

@pytest.fixture
def sut():
    return MarketTrades()

@pytest.fixture
def market_trades_data():
    return market_trades_fixture

def sanitize(d):
    # Convert any QuestDB timestamp objects to integers for easy comparison
    return {k: (v.value if hasattr(v, 'value') else v) for k, v in d.items()}

@pytest.mark.asyncio
class TestMarketTrades:

    async def test_create(self, pool, sut):
        await sut.create()

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT \"column\", \"type\" FROM table_columns(\'market_trades\')"
            )
        result = { r["column"]: r["type"] for r in rows }
        
        expected_schema = {
            'timestamp': 'TIMESTAMP',
            'product_id': 'SYMBOL',
            'trade_id': 'LONG',
            'price': 'DOUBLE',
            'size': 'DOUBLE',
            'side': 'SYMBOL',
        }
        
        for col, col_type in expected_schema.items():
            assert result.get(col) == col_type

    test_df = pd.DataFrame({
            'timestamp': [pd.Timestamp.now() + pd.Timedelta(minutes=i) for i in range(100)],
            'product_id': ['BTC-USD'] * 100,
            'trade_id': np.random.randint(0, 999_999_999, 100),
            'price': np.random.uniform(41000, 42000, 100),
            'size': np.random.uniform(.01, 5.0, 100),
            'side': np.random.choice(['BUY', 'SELL'], 100),
        })
    @pytest.mark.parametrize("data, expected", [
        ({"missing": 69}, MultipleInvalid), 
        ({
            "timestamp": TimestampNanos(1766721686000000000),
            "product_id": "BTC-USD",
            "trade_id": 439_270_198,
            "price": 41678.0,
            "size": 1.0,
            "side": "SELL"
         }, {
            "timestamp": TimestampNanos(1766721686000000000),
            "product_id": "BTC-USD",
            "trade_id": 439_270_198,
            "price": 41678.0,
            "size": 1.0,
            "side": "SELL"
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
                pd.testing.assert_frame_equal(res, expected, check_dtype=False)

    async def test_ingest_success(self, sut, pool):
        sut.ingest(self.test_df)
        Base.sender.flush()
        await asyncio.sleep(1.0)
        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT * FROM market_trades")
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
            "product_id": "ETH-USD",
            "trade_id": 439_270_198,
            "price": 41678.0,
            "size": 1.0,
            "side": "SELL"
         }
        sut.insert(data)
        Base.sender.flush()
        await asyncio.sleep(1)
        async with pool.acquire() as conn:
            res = await conn.fetchrow("SELECT * FROM market_trades WHERE product_id = \'ETH-USD\'")
        assert res["product_id"] == "ETH-USD"
        assert res["price"] == 41678.0

    async def test_insert_fail(self, sut):
        sut.sender = MagicMock()
        sut.sender.row.side_effect = IngressError(420, "kirked!")
        with pytest.raises(IngressError):
            data = {
            "timestamp": TimestampNanos(1766721686000000000),
            "product_id": "ETH-USD",
            "trade_id": 439_270_198,
            "price": 41678.0,
            "size": 1.0,
            "side": "SELL"
            }
            sut.insert(data)

    async def test_call(self, sut, pool, market_trades_data):
        snapshot_msg = market_trades_data[0]

        sut(snapshot_msg)
        Base.sender.flush()
        await asyncio.sleep(1.0)

        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT * FROM market_trades")
        assert len(res) > 0

        if len(market_trades_data) > 1:
            update_msg = market_trades_data[1]
            sut(update_msg)
            Base.sender.flush()
            await asyncio.sleep(1.0)

            async with pool.acquire() as conn:
                res_after_update = await conn.fetch("SELECT * FROM market_trades")
            assert len(res_after_update) >= len(res)

    async def test_snapshot(self, sut, market_trades_data):
        msg = market_trades_data[0]
        event = msg['events'][0]
        event_data = {
            'trades': event['trades']
        }

        df = sut.snapshot(event_data)

        assert isinstance(df, pd.DataFrame)
        assert 'timestamp' in df.columns
        assert 'product_id' in df.columns
        assert 'trade_id' in df.columns
        assert 'price' in df.columns
        assert 'size' in df.columns
        assert 'side' in df.columns
        assert len(df) == len(event['trades'])

    async def test_update(self, sut, market_trades_data):
        # Find an update message
        update_msg = None
        for msg in market_trades_data:
            for event in msg['events']:
                if event['type'] == 'update':
                    update_msg = msg
                    break
            if update_msg:
                break

        if update_msg is None:
            pytest.skip("No update message found in fixture data")

        event = update_msg['events'][0]
        event_data = {
            'timestamp': update_msg['timestamp'],
            'trades': event['trades']
        }

        rows = list(sut.update(event_data))

        assert len(rows) == len(event['trades'])
        for row in rows:
            assert 'timestamp' in row
            assert isinstance(row['timestamp'], TimestampNanos)