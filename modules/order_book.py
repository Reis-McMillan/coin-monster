import logging
import math
import numpy as np
import pandas as pd
from questdb.ingress import TimestampNanos
from typing import Optional

from db import OrderBook as OrderBookDB

logger = logging.getLogger("coin-monster.order_book")

def _round_up(n):
    '''Rounds up to higest digit. Examples:
       4207 -> 5000,
       2100 -> 3000,
       2001 -> 3000,

    '''
    if n <= 0:
        return 0
    
    magnitude = 10 ** math.floor(math.log10(n))
    
    return math.ceil(n / magnitude) * magnitude

def _ensure_capacity(
        arr: np.ndarray,
        n_levels: int,
        max_levels: int
    ):

    if n_levels >= max_levels:
        new_max_levels = max(max_levels * 2, _round_up(n_levels))
        logger.debug("Order book capacity expanded: %d -> %d levels", max_levels, new_max_levels)
        new_arr = np.zeros((new_max_levels, 2), dtype=np.float64)
        new_arr[:arr.shape[0]] = arr
        return new_arr, new_max_levels
    else: return arr, max_levels
    

class OrderBook:
    order_book: Optional[OrderBookDB] = None

    @classmethod
    def set_order_book_db(cls, order_book):
        cls.order_book = order_book

    def __init__(self, coin: str, max_levels: int=1000):
        self.coin = coin
        self.max_levels_bid = max_levels
        self.max_levels_ask = max_levels
        self.n_levels_bid = 0
        self.n_levels_ask = 0
        self.bids = np.zeros((max_levels, 2), dtype=np.float64)
        self.asks = np.zeros((max_levels, 2), dtype=np.float64)
        self.last_sequence_num = -1

    def _parse_data(self, data: dict):
        ts = TimestampNanos(int(pd.Timestamp(data['timestamp']).value))
        bids = {}
        asks = {}
        for e in data['events']:
            for u in e['updates']:
                t = pd.Timestamp(u['event_time'])
                # bids
                if u['side'] == 'bid':
                    # check price level and event time
                    if u['price_level'] not in bids.keys():
                        bids[u['price_level']] = (u['new_quantity'], t)
                    elif t > bids[u['price_level']][1]:
                        bids[u['price_level']] = (u['new_quantity'], t)
                # asks (offer)
                else:
                    if u['price_level'] not in asks.keys():
                        asks[u['price_level']] = (u['new_quantity'], t)
                    elif t > asks[u['price_level']][1]:
                        asks[u['price_level']] = (u['new_quantity'], t)

        bid_arr = []
        for price_level, (quantity, _) in bids.items():
            bid_arr.append([price_level, quantity])
        bids = np.array(bid_arr, dtype=np.float64)

        ask_arr = []
        for price_level, (quantity, _) in asks.items():
            ask_arr.append([price_level, quantity])
        asks = np.array(ask_arr, dtype=np.float64)

        return ts, bids, asks

    def _update_book(self, bids: np.ndarray, asks: np.ndarray):
        # handle empty input arrays
        if bids.size == 0:
            bids = np.empty((0, 2), dtype=np.float64)
        if asks.size == 0:
            asks = np.empty((0, 2), dtype=np.float64)

        # separate actions
        keep_mask = bids[:, 1] > 0
        to_upsert = bids[keep_mask]
        to_delete = bids[~keep_mask, 0]

        # create placeholder for edits
        current_bids = self.bids[:self.n_levels_bid].copy()

        # delete and upsert
        if to_delete.size > 0:
            mask = ~np.isin(current_bids[:, 0], to_delete)
            current_bids = current_bids[mask]

        if to_upsert.size > 0:
            mask = ~np.isin(current_bids[:, 0], to_upsert[:, 0])
            current_bids = current_bids[mask]
            current_bids = np.vstack([current_bids, to_upsert])
        
        # sort
        sorted_indices = np.argsort(-current_bids[:, 0])
        current_bids = current_bids[sorted_indices]

        # update capacity if necessary
        self.bids, self.max_levels_bid = _ensure_capacity(
            self.bids, current_bids.shape[0], self.max_levels_bid
        )
        self.n_levels_bid = current_bids.shape[0]

        # update bids
        self.bids[:self.n_levels_bid] = current_bids

        # same logic as above for asks
        keep_mask = asks[:, 1] > 0
        to_upsert = asks[keep_mask]
        to_delete = asks[~keep_mask, 0]

        current_asks = self.asks[:self.n_levels_ask].copy()

        if to_delete.size > 0:
            mask = ~np.isin(current_asks[:, 0], to_delete)
            current_asks = current_asks[mask]

        if to_upsert.size > 0:
            mask = ~np.isin(current_asks[:, 0], to_upsert[:, 0])
            current_asks = current_asks[mask]
            current_asks = np.vstack([current_asks, to_upsert])
        
        sorted_indices = np.argsort(current_asks[:, 0])
        current_asks = current_asks[sorted_indices]

        self.asks, self.max_levels_ask = _ensure_capacity(
            self.asks, current_asks.shape[0], self.max_levels_ask
        )
        self.n_levels_ask = current_asks.shape[0]

        self.asks[:self.n_levels_ask] = current_asks

    def consume_message(self, data: dict):
        if data['sequence_num'] < self.last_sequence_num:
            logger.debug("Out-of-order message dropped for %s (seq %d < %d)", self.coin, data['sequence_num'], self.last_sequence_num)
            return
        else:
            self.last_sequence_num = data['sequence_num']
        ts, new_bids, new_asks = self._parse_data(data)

        self._update_book(new_bids, new_asks)

        self.order_book({
            'timestamp': ts,
            'product_id': self.coin,
            'bids': self.bids[:self.n_levels_bid],
            'asks': self.asks[:self.n_levels_ask]
        })