import asyncio
from datetime import datetime
import aiohttp
import asyncpg
import pytest
import pytest_asyncio
from questdb.ingress import IngressError, TimestampNanos
from voluptuous.error import Invalid, MultipleInvalid
import numpy as np
import numpy.testing as npt
from unittest.mock import MagicMock

from config import config
from db import Base, OrderBook

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
            max_inactive_connection_lifetime=300,  # Recycle connections after 5 min idle
            command_timeout=60,
        )
    Base.set_pool(pool)
    Base.establish() 
    
    yield pool  
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS order_book")
    await Base.close()


@pytest.fixture
def sut():
    return OrderBook()


def sanitize(d):
    # Convert any QuestDB timestamp objects to integers for easy comparison
    return {k: (v.value if hasattr(v, 'value') else v) for k, v in d.items()}


@pytest.mark.asyncio
class TestOrderBook:

    async def test_create(self, sut, pool):
        await sut.create()

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT \"column\", \"type\" FROM table_columns(\'order_book\')"
            )
        
        result = { r["column"]: r["type"] for r in rows }
        
        expected_schema = {
            'timestamp': 'TIMESTAMP',
            'product_id': 'SYMBOL',
            'bids': 'DOUBLE[][]',
            'asks': 'DOUBLE[][]'
        }
        
        for col, col_type in expected_schema.items():
            assert result.get(col) == col_type

    @pytest.mark.parametrize("data, expected", [
        ({'missing': 420}, MultipleInvalid),
        ({
            'timestamp': TimestampNanos(int(1770227237 * 1e9)),
            'product_id': 'BTC-USD',
            'bids': np.zeros((100,), dtype=np.int8),
            'asks': np.zeros((100,), dtype=np.int8)
        }, Invalid),
        ({
            'timestamp': TimestampNanos(int(1770227237 * 1e9)),
            'product_id': 'BTC-USD',
            'bids': np.full((100,2), 'a'),
            'asks': np.full((100,2), 'b')
        }, Invalid),
        ({
            'timestamp': TimestampNanos(int(1770227237 * 1e9)),
            'product_id': 'BTC-USD',
            'bids': np.zeros((100,3), dtype=np.int8),
            'asks': np.zeros((100,3), dtype=np.int8)
        }, Invalid),
        ({
            'timestamp': TimestampNanos(int(1770227237 * 1e9)),
            'product_id': 'BTC-USD',
            'bids': [[1, 2], [3, 4, 5]],
            'asks': np.zeros((100,2), dtype=np.int8)
        }, Invalid),
        ({
            'timestamp': TimestampNanos(int(1770227237 * 1e9)),
            'product_id': 'BTC-USD',
            'bids': np.zeros((100, 2), dtype=np.int8),
            'asks': np.zeros((100,2), dtype=np.int8)
        },
        {
            'timestamp': TimestampNanos(int(1770227237 * 1e9)),
            'product_id': 'BTC-USD',
            'bids': np.zeros((100, 2), dtype=np.float64),
            'asks': np.zeros((100,2), dtype=np.float64)
        })
    ])
    async def test_validate(self, sut, data, expected):
        if isinstance(expected, type) and issubclass(expected, Exception):
            with pytest.raises(expected):
                sut.validate(data)
        else:
            res = sut.validate(data)
            if isinstance(res, dict):
                res = sanitize(res)
                expected = sanitize(expected)
                assert res.keys() == expected.keys()
                assert res['timestamp'] == expected['timestamp']
                assert res['product_id'] == expected['product_id']
                npt.assert_allclose(res['asks'], expected['asks'])
                npt.assert_allclose(res['bids'], expected['bids'])
                assert res['asks'].dtype == expected['asks'].dtype
                assert res['bids'].dtype == expected['bids'].dtype

    async def test_insert_success(self, sut, pool):
        bids_col_0 = np.random.normal(75_000, 2_500, 500)
        bids_col_1 = np.random.exponential(2, 500)
        bids = np.column_stack((bids_col_0, bids_col_1))
        asks_col_0 = np.random.normal(75_500, 2_500, 500)
        asks_col_1 = np.random.exponential(2, 500)
        asks = np.column_stack((asks_col_0, asks_col_1))
        data = {
            'timestamp': TimestampNanos(int(1770227238 * 1e9)),
            'product_id': 'ETH-USD',
            'bids': bids,
            'asks': asks
        }
        sut.insert(data)
        Base.sender.flush()
        await asyncio.sleep(1)

        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://{config.DB_HOST}:9000/exec",
                params={"query": "SELECT * FROM order_book WHERE product_id = 'ETH-USD'"}
            ) as resp:
                json_res = await resp.json()

        # Parse QuestDB REST response into a dict
        columns = [col['name'] for col in json_res['columns']]
        row = json_res['dataset'][0]
        res = dict(zip(columns, row))

        ts = datetime.fromisoformat(res['timestamp'].replace('Z', '+00:00'))
        assert int(ts.timestamp() * 1e6) == int(1770227238 * 1e6)
        assert res['product_id'] == 'ETH-USD'
        res_asks = np.array(res['asks'], dtype=np.float64)
        res_bids = np.array(res['bids'], dtype=np.float64)
        assert res_asks.dtype == np.float64
        assert res_bids.dtype == np.float64
        npt.assert_allclose(res_asks, asks)
        npt.assert_allclose(res_bids, bids)

    async def test_insert_fail(self, sut):
        sut.sender = MagicMock()
        sut.sender.row.side_effect = IngressError(420, 'kirked!')
        
        with pytest.raises(IngressError):
            data = {
                'timestamp': TimestampNanos(int(1770227238 * 1e9)),
                'product_id': 'ETH-USD',
                'bids': np.zeros((200, 2), dtype=np.float64),
                'asks': np.zeros((200, 2), dtype=np.float64)
            }
            sut.insert(data)