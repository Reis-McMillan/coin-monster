import asyncio
import asyncpg
from contextlib import asynccontextmanager
import logging
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from config import config
from db import Base, Candles, Level2, MarketTrades, Ticker
from db import OrderBook as OrderBookDB
from modules.websocket import Websocket
from modules.logging import setup_logging, shutdown_logging
from middleware.authenticated import authenticate
from middleware.logging import RequestLoggingMiddleware

logger = logging.getLogger("coin-monster")


class DB:
    candles: Candles
    level2: Level2
    market_trades: MarketTrades
    order_book: OrderBookDB
    ticker: Ticker


async def initialize_db():
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
    Base.establish
    
    DB.candles = Candles()
    await DB.candles.create()
    DB.level2 = Level2()
    await DB.level2.create()
    DB.market_trades = MarketTrades()
    await DB.market_trades.create()
    DB.order_book = OrderBookDB()
    await DB.order_book.create()
    DB.ticker = Ticker()
    await DB.ticker.create()

    return DB()


class CoinSubscription(BaseModel):
    coin: str
    tasks: list[str]


class CoinResponse(BaseModel):
    coin: str
    message: str


class WebsocketStatus(BaseModel):
    coin: str
    main: str
    l2: str


def _task_status(task: asyncio.Task) -> str:
    if not task.done():
        return "running"
    if task.cancelled():
        return "cancelled"
    if task.exception() is not None:
        return "failed"
    return "done"


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logger.info("Coin Monster starting")

    app.state.db = await initialize_db()
    logger.info("Database initialized")
    app.state.websockets = {}
    app.state.order_books = {}

    Websocket.set_db(app.state.db)
    Base.establish()

    yield

    for coin in list(app.state.websockets.keys()):
        await cancel_coin_tasks(app, coin)

    await Base.close()

    logger.info("Coin Monster shutting down")
    shutdown_logging()


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestLoggingMiddleware)


async def cancel_coin_tasks(app: FastAPI, coin: str):
    if coin in app.state.websockets:
        logger.debug("Cancelling tasks for %s", coin)
        tasks = app.state.websockets[coin]
        for task in tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        del app.state.websockets[coin]
        logger.info("Tasks cancelled for %s", coin)


@app.post("/coins/{coin}", status_code=201, response_model=CoinResponse, dependencies=[Depends(authenticate)])
async def subscribe_coin(coin: str):
    if coin in app.state.websockets:
        logger.warning("Duplicate subscription request for %s", coin)
        raise HTTPException(
            status_code=409,
            detail=f"Already subscribed to {coin}"
        )

    logger.info("Subscribing to %s", coin)
    main_ws = Websocket(coin=coin, channels=['candles', 'market_trades', 'ticker'])
    l2_ws = Websocket(coin=coin, channels=['l2_data'])

    main_task = asyncio.create_task(main_ws.websocket(), name=f"{coin}_main")
    l2_task = asyncio.create_task(l2_ws.websocket(), name=f"{coin}_l2")

    app.state.websockets[coin] = {
        'main': main_task,
        'l2': l2_task
    }

    logger.info("Subscribed to %s", coin)
    return CoinResponse(coin=coin, message=f"Subscribed to {coin}")


@app.delete("/coins/{coin}", response_model=CoinResponse, dependencies=[Depends(authenticate)])
async def unsubscribe_coin(coin: str):
    if coin not in app.state.websockets:
        logger.warning("Unsubscribe requested for unknown coin %s", coin)
        raise HTTPException(
            status_code=404,
            detail=f"Not subscribed to {coin}"
        )

    logger.info("Unsubscribing from %s", coin)
    await cancel_coin_tasks(app, coin)

    return CoinResponse(coin=coin, message=f"Unsubscribed from {coin}")


@app.get("/coins/{coin}/status", response_model=WebsocketStatus, dependencies=[Depends(authenticate)])
async def coin_status(coin: str):
    if coin not in app.state.websockets:
        raise HTTPException(status_code=404, detail=f"Not subscribed to {coin}")
    tasks = app.state.websockets[coin]
    return WebsocketStatus(
        coin=coin,
        main=_task_status(tasks['main']),
        l2=_task_status(tasks['l2']),
    )


@app.get("/coins", response_model=list[str], dependencies=[Depends(authenticate)])
async def list_coins():
    return list(app.state.websockets.keys())
