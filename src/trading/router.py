"""Binance Demo Trading API Router — exposes DemoTrading as REST endpoints.

Usage in main.py:
 from src.trading.router import router as trading_router
 app.include_router(trading_router)
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from src.trading.demo import DemoTrading, Market

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/binance-demo", tags=["Binance Demo"])

# ── DemoTrading singleton ─────────────────────────────────────────────────────

_demo: DemoTrading | None = None


def _get_demo() -> DemoTrading:
    """Lazy-initialise DemoTrading from environment variables."""
    global _demo
    if _demo is None:
        try:
            _demo = DemoTrading.from_env()
        except Exception as exc:
            logger.exception("Failed to initialise DemoTrading")
            raise HTTPException(
                status_code=503,
                detail=f"DemoTrading init failed: {exc}",
            ) from exc
    return _demo


def _run(coro):
    """Run an async coroutine from sync FastAPI endpoint (thread-safe)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


def _market(m: str) -> Market:
    return Market.SPOT if m == "spot" else Market.FUTURES


# ── Enums ──────────────────────────────────────────────────────────────────────


class MarketType(StrEnum):
    spot = "spot"
    futures = "futures"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


# ── Request models ─────────────────────────────────────────────────────────────


class PlaceOrderRequest(BaseModel):
    symbol: str = Field(..., description="Trading pair, e.g. BTCUSDT")
    side: OrderSide = Field(..., description="BUY or SELL")
    type: OrderType = Field(OrderType.LIMIT, description="Order type")
    quantity: float = Field(..., gt=0, description="Order quantity")
    price: float | None = Field(
        None, gt=0, description="Limit price (required for LIMIT orders)"
    )
    market: MarketType = Field(MarketType.spot, description="spot or futures")


# ── Response models ────────────────────────────────────────────────────────────


class PingResponse(BaseModel):
    status: str = "ok"


class TimeResponse(BaseModel):
    server_time: int
    iso: str


class AccountResponse(BaseModel):
    account_info: dict[str, Any]
    balances: list[dict[str, Any]]


class MarketPriceResponse(BaseModel):
    symbol: str
    price: float


class OrderBookResponse(BaseModel):
    symbol: str
    bids: list[list[Any]]
    asks: list[list[Any]]
    last_update_id: int | None = None


class KlineResponse(BaseModel):
    symbol: str
    interval: str
    klines: list[list[Any]]


class OpenOrdersResponse(BaseModel):
    symbol: str
    orders: list[dict[str, Any]]


class PositionsResponse(BaseModel):
    symbol: str | None = None
    positions: list[dict[str, Any]]


class TradeHistoryResponse(BaseModel):
    symbol: str
    trades: list[dict[str, Any]]


class OrderResponse(BaseModel):
    order: dict[str, Any]


class CancelOrderResponse(BaseModel):
    result: dict[str, Any]


class ExchangeInfoResponse(BaseModel):
    market: str
    exchange_info: dict[str, Any]


# ── Endpoints ──────────────────────────────────────────────────────────────────


@router.get("/ping", response_model=PingResponse)
def ping():
    """Connectivity test."""
    return PingResponse()


@router.get("/time", response_model=TimeResponse)
def server_time():
    """Server time in milliseconds and ISO format."""
    now_ms = int(time.time() * 1000)
    return TimeResponse(
        server_time=now_ms,
        iso=datetime.now(tz=UTC).isoformat(),
    )


@router.get("/account", response_model=AccountResponse)
def get_account():
    """Account info and balances."""
    demo = _get_demo()
    try:
        info = _run(demo.account_info())
        bals = _run(demo.balances())
    except Exception as exc:
        logger.exception("Account lookup failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return AccountResponse(account_info=info, balances=bals)


@router.get("/market-price", response_model=MarketPriceResponse)
def get_market_price(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT"),
):
    """Current market price for a symbol."""
    demo = _get_demo()
    try:
        result = _run(demo.market_price(symbol))
    except Exception as exc:
        logger.exception("Market price lookup failed for %s", symbol)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return MarketPriceResponse(symbol=symbol, price=float(result.get("price", 0)))


@router.get("/order-book", response_model=OrderBookResponse)
def get_order_book(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT"),
    limit: int = Query(10, ge=1, le=1000, description="Depth limit"),
):
    """Order book depth."""
    demo = _get_demo()
    try:
        book = _run(demo.order_book(symbol, limit=limit))
    except Exception as exc:
        logger.exception("Order book lookup failed for %s", symbol)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return OrderBookResponse(
        symbol=symbol,
        bids=book.get("bids", []),
        asks=book.get("asks", []),
        last_update_id=book.get("lastUpdateId"),
    )


@router.get("/klines", response_model=KlineResponse)
def get_klines(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT"),
    interval: str = Query("1h", description="Kline interval: 1m, 5m, 15m, 1h, 4h, 1d"),
    limit: int = Query(100, ge=1, le=1000, description="Number of klines"),
):
    """Kline/candlestick data."""
    demo = _get_demo()
    try:
        data = _run(demo.klines(symbol, interval=interval, limit=limit))
    except Exception as exc:
        logger.exception("Klines lookup failed for %s %s", symbol, interval)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return KlineResponse(symbol=symbol, interval=interval, klines=data)


@router.get("/open-orders", response_model=OpenOrdersResponse)
def get_open_orders(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT"),
    market: MarketType = Query(MarketType.spot, description="spot or futures"),
):
    """Current open orders for a symbol."""
    demo = _get_demo()
    try:
        orders = _run(demo.open_orders(symbol, market=_market(market.value)))
    except Exception as exc:
        logger.exception("Open orders lookup failed for %s", symbol)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if isinstance(orders, dict):
        orders = [orders]
    return OpenOrdersResponse(symbol=symbol, orders=orders)


@router.get("/positions", response_model=PositionsResponse)
def get_positions(
    symbol: str | None = Query(None, description="Trading pair, e.g. BTCUSDT"),
):
    """Futures open positions, optionally filtered by symbol."""
    demo = _get_demo()
    try:
        positions = _run(demo.positions(symbol=symbol))
    except Exception as exc:
        logger.exception("Positions lookup failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return PositionsResponse(symbol=symbol, positions=positions)


@router.get("/trade-history", response_model=TradeHistoryResponse)
def get_trade_history(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT"),
    market: MarketType = Query(MarketType.spot, description="spot or futures"),
):
    """Trade history for a symbol."""
    demo = _get_demo()
    try:
        trades = _run(demo.trade_history(symbol, market=_market(market.value)))
    except Exception as exc:
        logger.exception("Trade history lookup failed for %s", symbol)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return TradeHistoryResponse(symbol=symbol, trades=trades)


@router.post("/order", response_model=OrderResponse)
def place_order(body: PlaceOrderRequest):
    """Place a spot or futures order."""
    demo = _get_demo()

    if body.type == OrderType.LIMIT and body.price is None:
        raise HTTPException(
            status_code=422,
            detail="price is required for LIMIT orders",
        )

    try:
        if body.market == MarketType.spot:
            result = _run(
                demo.spot_order(
                    symbol=body.symbol,
                    side=body.side.value,
                    order_type=body.type.value,
                    quantity=body.quantity,
                    price=body.price,
                )
            )
        else:
            result = _run(
                demo.futures_order(
                    symbol=body.symbol,
                    side=body.side.value,
                    order_type=body.type.value,
                    quantity=body.quantity,
                    price=body.price,
                )
            )
    except Exception as exc:
        logger.exception("Order placement failed: %s", body)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return OrderResponse(order=result)


@router.delete("/order", response_model=CancelOrderResponse)
def cancel_order(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT"),
    order_id: int = Query(..., description="Order ID to cancel"),
    market: MarketType = Query(MarketType.spot, description="spot or futures"),
):
    """Cancel an open order."""
    demo = _get_demo()
    try:
        result = _run(
            demo.cancel_order(
                symbol=symbol,
                order_id=order_id,
                market=_market(market.value),
            )
        )
    except Exception as exc:
        logger.exception("Order cancellation failed: %s/%s", symbol, order_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return CancelOrderResponse(result=result)


@router.get("/exchange-info", response_model=ExchangeInfoResponse)
def get_exchange_info(
    market: MarketType = Query(MarketType.spot, description="spot or futures"),
):
    """Exchange trading rules and symbol information."""
    demo = _get_demo()
    try:
        info = _run(demo.exchange_info(market=_market(market.value)))
    except Exception as exc:
        logger.exception("Exchange info lookup failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return ExchangeInfoResponse(market=market.value, exchange_info=info)
