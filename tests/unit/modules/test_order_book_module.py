import asyncio
import asyncpg
import aiohttp
import pytest
import numpy as np
import numpy.testing as npt
from unittest.mock import patch, MagicMock
from questdb.ingress import TimestampNanos, IngressError

from modules.order_book import OrderBook, _ensure_capacity, _round_up
from db import Base
from db import OrderBook as OrderBookDB
from config import config

from tests.fixtures.websocket import level2 as level2_fixtures


@pytest.fixture(scope='module')
async def set_pool():
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

    yield

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP TABLE IF EXISTS order_book")
    await Base.close()

@pytest.fixture
# unused set_pool parameter ensures that Base's
# connection pool is established for create()
async def order_book_db(set_pool):
    db = OrderBookDB()
    await db.create()

    return db


@pytest.fixture
def sut(order_book_db):
    OrderBook.set_order_book_db(order_book_db)
    return OrderBook(coin='BTC-USD', max_levels=100)


@pytest.fixture
def l2_message():
    return level2_fixtures[0]


class TestRoundUp: 
    def test_round_up(self):
        res = _round_up(2701)
        assert res == 3000
    
    def test_no_round_up(self):
        res = _round_up(1000)
        assert res == 1000


class TestEnsureCapacity:

    def test_no_resize_needed(self):
        arr = np.zeros((10, 2), dtype=np.float64)
        new_arr, new_max = _ensure_capacity(arr, 5, 10)

        assert new_max == 10
        assert new_arr is arr

    def test_resize_at_boundary(self):
        arr = np.zeros((10, 2), dtype=np.float64)
        new_arr, new_max = _ensure_capacity(arr, 9, 10)

        assert new_max == 10
        assert new_arr is arr

    def test_resize_needed(self):
        arr = np.zeros((10, 2), dtype=np.float64)
        arr[:5] = [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]

        new_arr, new_max = _ensure_capacity(arr, 10, 10)

        assert new_max == 20
        assert new_arr.shape == (20, 2)
        npt.assert_array_equal(new_arr[:5], arr[:5])

    def test_resize_preserves_data(self):
        arr = np.zeros((4, 2), dtype=np.float64)
        arr[:] = [[1, 2], [3, 4], [5, 6], [7, 8]]

        new_arr, new_max = _ensure_capacity(arr, 4, 4)

        assert new_max == 8
        npt.assert_array_equal(new_arr[:4], arr)
        npt.assert_array_equal(new_arr[4:], np.zeros((4, 2)))

    def test_resize_max_condition(self):
        arr = np.zeros((200, 2), dtype=np.float64)
        new_arr, new_max = _ensure_capacity(arr, 2701, 200)
        assert new_arr.shape[0] == 3000
        assert new_max == 3000

    def test_resize_max_condition_other(self):
        arr = np.zeros((2000, 2), dtype=np.float64)
        new_arr, new_max = _ensure_capacity(arr, 2701, 2000)
        assert new_arr.shape[0] == 4000
        assert new_max == 4000

class TestOrderBookInit:

    def test_init_default(self, order_book_db):
        OrderBook.set_order_book_db(order_book_db)
        ob = OrderBook(coin='BTC-USD')

        assert ob.coin == 'BTC-USD'
        assert ob.max_levels_bid == 1000
        assert ob.max_levels_ask == 1000
        assert ob.n_levels_bid == 0
        assert ob.n_levels_ask == 0
        assert ob.bids.shape == (1000, 2)
        assert ob.asks.shape == (1000, 2)

    def test_init_custom_max_levels(self, order_book_db):
        OrderBook.set_order_book_db(order_book_db)
        ob = OrderBook(coin='ETH-USD', max_levels=50)

        assert ob.coin == 'ETH-USD'
        assert ob.max_levels_bid == 50
        assert ob.max_levels_ask == 50
        assert ob.bids.shape == (50, 2)
        assert ob.asks.shape == (50, 2)

    def test_init_arrays_zeroed(self, sut):
        npt.assert_array_equal(sut.bids, np.zeros((100, 2), dtype=np.float64))
        npt.assert_array_equal(sut.asks, np.zeros((100, 2), dtype=np.float64))


class TestParseData:

    def test_parse_data_basic(self, sut, l2_message):
        ts, bids, asks = sut._parse_data(l2_message)

        assert isinstance(ts, TimestampNanos)
        assert bids.shape[1] == 2
        assert asks.shape[1] == 2
        assert len(bids) == 20187
        assert len(asks) == 28462

    def test_parse_data_bid_values(self, sut, l2_message):
        _, bids, _ = sut._parse_data(l2_message)

        bid_prices = bids[:, 0]
        bid_prices_unique = set(bid_prices)
        assert len(bid_prices) == len(bid_prices_unique)
        assert bid_prices[0] == 87472.69
        assert bid_prices[1] == 87472.42
        assert bid_prices[2] == 87471.38
        assert bid_prices[3] == 87470.71
        assert bid_prices[4] == 87470.69
        assert bid_prices[5] == 87470.37
        assert bid_prices[6] == 87470.08
        assert bid_prices[7] == 87468.96
        assert bid_prices[8] == 87468.94
        assert bid_prices[9] == 87468.52

    def test_parse_data_ask_values(self, sut, l2_message):
        _, _, asks = sut._parse_data(l2_message)

        ask_prices = asks[:, 0]
        ask_prices_unique = set(ask_prices)
        assert len(ask_prices) == len(ask_prices_unique)
        assert ask_prices[0] == 87472.7
        assert ask_prices[1] == 87472.71
        assert ask_prices[2] == 87472.72
        assert ask_prices[3] == 87473.59
        assert ask_prices[4] == 87474.
        assert ask_prices[5] == 87474.01
        assert ask_prices[6] == 87474.09
        assert ask_prices[7] == 87474.78
        assert ask_prices[8] == 87475.98
        assert ask_prices[9] == 87476.


    def test_parse_data_latest_event_time_wins(self, sut):
        message = {
            "channel": "l2_data",
            "timestamp": "2025-12-28T20:58:55.156352116Z",
            "events": [{
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {"side": "bid", "event_time": "2025-12-28T20:58:54.000000Z", "price_level": "100.00", "new_quantity": "1.0"},
                    {"side": "bid", "event_time": "2025-12-28T20:58:55.000000Z", "price_level": "100.00", "new_quantity": "2.0"},
                ]
            }]
        }

        _, bids, _ = sut._parse_data(message)

        assert len(bids) == 1
        assert bids[0, 0] == 100.00
        assert bids[0, 1] == 2.0

    def test_parse_data_earlier_event_time_ignored(self, sut):
        message = {
            "channel": "l2_data",
            "timestamp": "2025-12-28T20:58:55.156352116Z",
            "events": [{
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {"side": "offer", "event_time": "2025-12-28T20:58:55.000000Z", "price_level": "101.00", "new_quantity": "5.0"},
                    {"side": "offer", "event_time": "2025-12-28T20:58:54.000000Z", "price_level": "101.00", "new_quantity": "1.0"},
                ]
            }]
        }

        _, _, asks = sut._parse_data(message)

        assert len(asks) == 1
        assert asks[0, 1] == 5.0

    def test_parse_data_empty_events(self, sut):
        message = {
            "channel": "l2_data",
            "timestamp": "2025-12-28T20:58:55.156352116Z",
            "events": [{"type": "snapshot", "product_id": "BTC-USD", "updates": []}]
        }

        _, bids, asks = sut._parse_data(message)

        assert bids.size == 0
        assert asks.size == 0

    def test_parse_data_multiple_events(self, sut):
        message = {
            "channel": "l2_data",
            "timestamp": "2025-12-28T20:58:55.156352116Z",
            "events": [
                {"type": "update", "product_id": "BTC-USD", "updates": [
                    {"side": "bid", "event_time": "2025-12-28T20:58:54.000000Z", "price_level": "100.00", "new_quantity": "1.0"},
                ]},
                {"type": "update", "product_id": "BTC-USD", "updates": [
                    {"side": "bid", "event_time": "2025-12-28T20:58:55.000000Z", "price_level": "99.00", "new_quantity": "2.0"},
                ]},
            ]
        }

        _, bids, _ = sut._parse_data(message)

        assert len(bids) == 2

    def test_parse_data_bids_only(self, sut):
        message = {
            "channel": "l2_data",
            "timestamp": "2025-12-28T20:58:55.156352116Z",
            "events": [{"type": "snapshot", "product_id": "BTC-USD", "updates": [
                {"side": "bid", "event_time": "2025-12-28T20:58:54.759526Z", "price_level": "100.00", "new_quantity": "1.0"},
            ]}]
        }

        _, bids, asks = sut._parse_data(message)

        assert len(bids) == 1
        assert asks.size == 0

    def test_parse_data_asks_only(self, sut):
        message = {
            "channel": "l2_data",
            "timestamp": "2025-12-28T20:58:55.156352116Z",
            "events": [{"type": "snapshot", "product_id": "BTC-USD", "updates": [
                {"side": "offer", "event_time": "2025-12-28T20:58:54.759526Z", "price_level": "101.00", "new_quantity": "1.0"},
            ]}]
        }

        _, bids, asks = sut._parse_data(message)

        assert bids.size == 0
        assert len(asks) == 1


class TestUpdateBook:

    def test_update_book_insert(self, sut):
        bids = np.array([[100.0, 1.5], [99.0, 2.0]], dtype=np.float64)
        asks = np.array([[101.0, 1.0], [102.0, 3.0]], dtype=np.float64)

        sut._update_book(bids, asks)

        assert sut.n_levels_bid == 2
        assert sut.n_levels_ask == 2
        npt.assert_array_equal(sut.bids[0], [100.0, 1.5])
        npt.assert_array_equal(sut.bids[1], [99.0, 2.0])
        npt.assert_array_equal(sut.asks[0], [101.0, 1.0])
        npt.assert_array_equal(sut.asks[1], [102.0, 3.0])

    def test_update_book_sorted_bids_descending(self, sut):
        bids = np.array([[99.0, 1.0], [101.0, 2.0], [100.0, 1.5]], dtype=np.float64)
        asks = np.empty((0, 2), dtype=np.float64)

        sut._update_book(bids, asks)

        assert sut.bids[0, 0] == 101.0
        assert sut.bids[1, 0] == 100.0
        assert sut.bids[2, 0] == 99.0

    def test_update_book_sorted_asks_ascending(self, sut):
        bids = np.empty((0, 2), dtype=np.float64)
        asks = np.array([[103.0, 1.0], [101.0, 2.0], [102.0, 1.5]], dtype=np.float64)

        sut._update_book(bids, asks)

        assert sut.asks[0, 0] == 101.0
        assert sut.asks[1, 0] == 102.0
        assert sut.asks[2, 0] == 103.0

    def test_update_book_delete_zero_quantity(self, sut):
        bids = np.array([[100.0, 1.5], [99.0, 2.0]], dtype=np.float64)
        asks = np.array([[101.0, 1.0]], dtype=np.float64)
        sut._update_book(bids, asks)

        delete_bids = np.array([[100.0, 0.0]], dtype=np.float64)
        sut._update_book(delete_bids, np.empty((0, 2), dtype=np.float64))

        assert sut.n_levels_bid == 1
        assert sut.bids[0, 0] == 99.0

    def test_update_book_delete_nonexistent_price(self, sut):
        bids = np.array([[100.0, 1.5]], dtype=np.float64)
        sut._update_book(bids, np.empty((0, 2), dtype=np.float64))

        delete_bids = np.array([[999.0, 0.0]], dtype=np.float64)
        sut._update_book(delete_bids, np.empty((0, 2), dtype=np.float64))

        assert sut.n_levels_bid == 1
        assert sut.bids[0, 0] == 100.0

    def test_update_book_delete_all_levels(self, sut):
        bids = np.array([[100.0, 1.5], [99.0, 2.0]], dtype=np.float64)
        sut._update_book(bids, np.empty((0, 2), dtype=np.float64))

        delete_bids = np.array([[100.0, 0.0], [99.0, 0.0]], dtype=np.float64)
        sut._update_book(delete_bids, np.empty((0, 2), dtype=np.float64))

        assert sut.n_levels_bid == 0

    def test_update_book_upsert_existing_price(self, sut):
        bids = np.array([[100.0, 1.5]], dtype=np.float64)
        sut._update_book(bids, np.empty((0, 2), dtype=np.float64))

        update_bids = np.array([[100.0, 3.0]], dtype=np.float64)
        sut._update_book(update_bids, np.empty((0, 2), dtype=np.float64))

        assert sut.n_levels_bid == 1
        assert sut.bids[0, 0] == 100.0
        assert sut.bids[0, 1] == 3.0

    def test_update_book_mixed_upsert_and_delete(self, sut):
        bids = np.array([[100.0, 1.0], [99.0, 2.0], [98.0, 3.0]], dtype=np.float64)
        sut._update_book(bids, np.empty((0, 2), dtype=np.float64))

        update_bids = np.array([
            [100.0, 0.0],   # delete
            [99.0, 5.0],    # update
            [97.0, 1.0],    # insert
        ], dtype=np.float64)
        sut._update_book(update_bids, np.empty((0, 2), dtype=np.float64))

        assert sut.n_levels_bid == 3
        assert sut.bids[0, 0] == 99.0
        assert sut.bids[0, 1] == 5.0
        assert sut.bids[1, 0] == 98.0
        assert sut.bids[2, 0] == 97.0

    def test_update_book_empty_input(self, sut):
        bids = np.array([[100.0, 1.5]], dtype=np.float64)
        sut._update_book(bids, np.empty((0, 2), dtype=np.float64))

        sut._update_book(np.empty((0, 2), dtype=np.float64), np.empty((0, 2), dtype=np.float64))

        assert sut.n_levels_bid == 1
        assert sut.bids[0, 0] == 100.0

    def test_update_book_capacity_expansion(self):
        ob = OrderBook(coin='BTC-USD', max_levels=2)

        bids = np.array([[100.0, 1.0], [99.0, 1.0], [98.0, 1.0]], dtype=np.float64)
        ob._update_book(bids, np.empty((0, 2), dtype=np.float64))

        assert ob.max_levels_bid == 4
        assert ob.n_levels_bid == 3

    def test_update_book_ask_capacity_expansion(self):
        ob = OrderBook(coin='BTC-USD', max_levels=2)

        asks = np.array([[101.0, 1.0], [102.0, 1.0], [103.0, 1.0]], dtype=np.float64)
        ob._update_book(np.empty((0, 2), dtype=np.float64), asks)

        assert ob.max_levels_ask == 4
        assert ob.n_levels_ask == 3


@pytest.mark.asyncio
class TestConsumeMessage:

    async def test_consume_message(self, sut, l2_message):
        sut.consume_message(l2_message)
        Base.sender.flush()

        await asyncio.sleep(1)

        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://{config.DB_HOST}:9000/exec",
                params={"query": "SELECT bids, asks FROM order_book WHERE product_id = 'BTC-USD'"}
            ) as resp:
                json_res = await resp.json()

        bids = np.array(json_res['dataset'][0][0], dtype=np.float64)
        asks = np.array(json_res['dataset'][0][1], dtype=np.float64)

        npt.assert_array_equal(bids, sut.bids[:sut.n_levels_bid])
        npt.assert_array_equal(asks, sut.asks[:sut.n_levels_ask])

    async def test_consume_message_sequential(self, sut):
        # change product_id for test
        sut.coin = 'SOL-USD'
        msg1 = {
            "channel": "l2_data",
            "sequence_num": 99_999,
            "timestamp": "2025-12-28T20:58:55.156352116Z",
            "events": [{"type": "snapshot", "product_id": "SOL-USD", "updates": [
                {"side": "bid", "event_time": "2025-12-28T20:58:54.759526Z", "price_level": "100.00", "new_quantity": "1.0"},
            ]}]
        }
        msg2 = {
            "channel": "l2_data",
            "sequence_num": 100_000,
            "timestamp": "2025-12-28T20:58:56.156352116Z",
            "events": [{"type": "update", "product_id": "SOL-USD", "updates": [
                {"side": "bid", "event_time": "2025-12-28T20:58:55.759526Z", "price_level": "99.00", "new_quantity": "2.0"},
            ]}]
        }

        sut.consume_message(msg1)
        sut.consume_message(msg2)
        Base.sender.flush()

        await asyncio.sleep(1)

        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://{config.DB_HOST}:9000/exec",
                params={"query": "SELECT bids FROM order_book WHERE product_id = \'SOL-USD\'"}
            ) as resp:
                json_res = await resp.json()

        bids_t_0 = np.array(json_res['dataset'][0][0], dtype=np.float64)
        bids_t_1 = np.array(json_res['dataset'][1][0], dtype=np.float64)
        npt.assert_array_equal(bids_t_0, np.array([[100.0, 1.0]]))
        npt.assert_array_equal(bids_t_1, np.array([[100.0, 1.0], [99.0, 2.0]]))

    async def test_consume_message_db_error(self, sut, l2_message):
        sut.order_book.sender = MagicMock()
        sut.order_book.sender.row.side_effect = IngressError(420, 'kirked!')

        with pytest.raises(IngressError, match="kirked!"):
            sut.consume_message(l2_message)


class TestSetOrderBookDB:

    def test_set_order_book_db(self):
        mock_db = MagicMock()
        OrderBook.set_order_book_db(mock_db)

        assert OrderBook.order_book is mock_db
