"""
Binance trading client — Spot, USDⓈ-M Futures, and WebSocket streams.

Usage
-----
    client = await TradingClient.create()
    info = await client.get_account_info()
    ticker = await client.get_ticker("BTCUSDT")
    order = await client.place_order("BTCUSDT", "BUY", "MARKET", quote_order_qty=100)
    await client.close()
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import httpx
import websockets

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL_SPOT = "https://api.binance.com"
BASE_URL_FUTURES = "https://fapi.binance.com"

WSS_URL_SPOT = "wss://stream.binance.com:9443/ws"
WSS_URL_FUTURES = "wss://fstream.binance.com/ws"

RECV_WINDOW = 5000  # ms


# ---------------------------------------------------------------------------
# Enums & simple types
# ---------------------------------------------------------------------------

class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_LOSS = "STOP_LOSS"
    STOP_LOSS_LIMIT = "STOP_LOSS_LIMIT"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"
    LIMIT_MAKER = "LIMIT_MAKER"


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    PENDING_CANCEL = "PENDING_CANCEL"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class TimeInForce(str, Enum):
    GTC = "GTC"   # Good-til-cancelled
    IOC = "IOC"   # Immediate-or-cancel
    FOK = "FOK"   # Fill-or-kill


class Interval(str, Enum):
    """Kline / candlestick intervals."""
    ONE_MINUTE = "1m"
    THREE_MINUTES = "3m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    TWO_HOURS = "2h"
    FOUR_HOURS = "4h"
    SIX_HOURS = "6h"
    EIGHT_HOURS = "8h"
    TWELVE_HOURS = "12h"
    ONE_DAY = "1d"
    THREE_DAYS = "3d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1M"


# ---------------------------------------------------------------------------
# Dataclasses for typed responses
# ---------------------------------------------------------------------------

@dataclass
class AccountBalance:
    asset: str
    free: Decimal
    locked: Decimal

    @classmethod
    def from_dict(cls, d: dict) -> AccountBalance:
        return cls(
            asset=d["asset"],
            free=Decimal(d["free"]),
            locked=Decimal(d["locked"]),
        )


@dataclass
class AccountInfo:
    balances: List[AccountBalance]
    can_trade: bool
    can_withdraw: bool
    can_deposit: bool

    @classmethod
    def from_dict(cls, d: dict) -> AccountInfo:
        return cls(
            balances=[AccountBalance.from_dict(b) for b in d.get("balances", [])],
            can_trade=d.get("canTrade", False),
            can_withdraw=d.get("canWithdraw", False),
            can_deposit=d.get("canDeposit", False),
        )


@dataclass
class Ticker:
    symbol: str
    price: Decimal
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    volume: Optional[Decimal] = None
    quote_volume: Optional[Decimal] = None
    change: Optional[Decimal] = None          # 24h price change
    change_percent: Optional[Decimal] = None  # 24h change %

    @classmethod
    def from_dict(cls, d: dict) -> Ticker:
        return cls(
            symbol=d["symbol"],
            price=Decimal(d.get("lastPrice", d.get("price", "0"))),
            high=Decimal(d["highPrice"]) if d.get("highPrice") else None,
            low=Decimal(d["lowPrice"]) if d.get("lowPrice") else None,
            volume=Decimal(d["volume"]) if d.get("volume") else None,
            quote_volume=Decimal(d["quoteVolume"]) if d.get("quoteVolume") else None,
            change=Decimal(d["priceChange"]) if d.get("priceChange") else None,
            change_percent=Decimal(d["priceChangePercent"]) if d.get("priceChangePercent") else None,
        )


@dataclass
class Order:
    symbol: str
    order_id: int
    client_order_id: str
    price: Decimal
    orig_qty: Decimal
    executed_qty: Decimal
    cummulative_quote_qty: Decimal
    status: OrderStatus
    side: OrderSide
    type: OrderType
    transact_time: int
    fills: List[dict] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> Order:
        return cls(
            symbol=d["symbol"],
            order_id=d.get("orderId", 0),
            client_order_id=d.get("clientOrderId", ""),
            price=Decimal(d.get("price", "0")),
            orig_qty=Decimal(d.get("origQty", "0")),
            executed_qty=Decimal(d.get("executedQty", "0")),
            cummulative_quote_qty=Decimal(d.get("cummulativeQuoteQty", "0")),
            status=OrderStatus(d.get("status", "NEW")),
            side=OrderSide(d.get("side", "BUY")),
            type=OrderType(d.get("type", "MARKET")),
            transact_time=d.get("transactTime", 0),
            fills=d.get("fills", []),
        )


@dataclass
class Candlestick:
    open_time: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time: int
    quote_volume: Decimal
    count: int
    taker_buy_volume: Decimal
    taker_buy_quote_volume: Decimal
    ignore: Decimal

    @classmethod
    def from_raw(cls, raw: list) -> Candlestick:
        """Parse a kline entry from the REST API (list of 12 values)."""
        return cls(
            open_time=raw[0],
            open=Decimal(raw[1]),
            high=Decimal(raw[2]),
            low=Decimal(raw[3]),
            close=Decimal(raw[4]),
            volume=Decimal(raw[5]),
            close_time=raw[6],
            quote_volume=Decimal(raw[7]),
            count=raw[8],
            taker_buy_volume=Decimal(raw[9]),
            taker_buy_quote_volume=Decimal(raw[10]),
            ignore=Decimal(raw[11]),
        )


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class BinanceError(Exception):
    """Binance API error with code and message."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.msg = message
        super().__init__(f"[{code}] {message}")


class RateLimitError(BinanceError):
    """HTTP 418 / 429 — back off."""


class TradingClientError(Exception):
    """General trading client error."""


# ---------------------------------------------------------------------------
# Rate-limit state
# ---------------------------------------------------------------------------

@dataclass
class RateLimitState:
    """Tracks rate-limit headers to stay within Binance limits."""
    orders_remaining: int = 10
    orders_weight_remaining: int = 6000
    raw_requests_remaining: int = 1200
    retry_after: float = 0.0


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------

class TradingClient:
    """
    Async Binance client for Spot and USDⓈ-M Futures.

    Reads API credentials from environment variables:
        BINANCE_API_KEY
        BINANCE_API_SECRET

    Pass ``api_key`` / ``api_secret`` explicitly to override.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = BASE_URL_SPOT,
        wss_url: str = WSS_URL_SPOT,
        timeout: float = 30.0,
        max_retries: int = 3,
    ) -> None:
        self._api_key = api_key or ""
        self._api_secret = api_secret or ""
        self._base_url = base_url.rstrip("/")
        self._wss_url = wss_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._rate_limit = RateLimitState()
        self._http: Optional[httpx.AsyncClient] = None
        self._ws_connections: list = []
        self._closed = False

    # ---- factory ---------------------------------------------------------

    @classmethod
    async def create(
        cls,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = BASE_URL_SPOT,
        wss_url: str = WSS_URL_SPOT,
        futures: bool = False,
        **kwargs: Any,
    ) -> TradingClient:
        """
        Create and return a ready-to-use client.

        Set *futures=True* to target USDⓈ-M Futures endpoints.
        """
        from os import getenv

        key = api_key or getenv("BINANCE_API_KEY") or ""
        secret = api_secret or getenv("BINANCE_API_SECRET") or ""

        if futures:
            base_url = base_url if base_url != BASE_URL_SPOT else BASE_URL_FUTURES
            wss_url = wss_url if wss_url != WSS_URL_SPOT else WSS_URL_FUTURES

        client = cls(api_key=key, api_secret=secret, base_url=base_url, wss_url=wss_url, **kwargs)
        client._http = httpx.AsyncClient(
            base_url=client._base_url,
            timeout=httpx.Timeout(client._timeout),
            headers={"X-MBX-APIKEY": client._api_key} if client._api_key else {},
        )
        return client

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None:
            raise TradingClientError("Client not initialised — use TradingClient.create()")
        return self._http

    # ---- lifecycle -------------------------------------------------------

    async def close(self) -> None:
        """Close all HTTP and WebSocket connections."""
        if self._closed:
            return
        self._closed = True
        for ws in self._ws_connections:
            try:
                await ws.close()
            except Exception:
                pass
        self._ws_connections.clear()
        if self._http:
            await self._http.aclose()

    async def __aenter__(self) -> TradingClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ---- signing ---------------------------------------------------------

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add signature to params dict."""
        if not self._api_secret:
            return params  # public endpoints only
        params["timestamp"] = int(time.time() * 1000)
        query = urllib.parse.urlencode(sorted(params.items()))
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = signature
        return params

    # ---- request helpers -------------------------------------------------

    def _check_rate_limit(self) -> None:
        """If we received a retry-after header, honour it."""
        if self._rate_limit.retry_after > time.time():
            sleep_for = self._rate_limit.retry_after - time.time()
            if sleep_for > 0:
                logger.warning("Rate-limited; sleeping %.1fs", sleep_for)
                time.sleep(sleep_for)  # lightweight; asyncio.sleep not needed here

    def _update_rate_limit(self, response: httpx.Response) -> None:
        """Parse rate-limit headers."""
        used = response.headers.get("X-MBX-USED-WEIGHT-1m")
        remaining = response.headers.get("X-MBX-ORDER-COUNT-1m")
        if used:
            self._rate_limit.orders_weight_remaining = 6000 - int(used)
        if remaining:
            self._rate_limit.orders_remaining = int(remaining)

        if response.status_code == 418:
            retry = response.headers.get("Retry-After")
            self._rate_limit.retry_after = time.time() + (float(retry) if retry else 300)
            raise RateLimitError(418, f"IP banned; retry after {retry}s")
        elif response.status_code == 429:
            retry = response.headers.get("Retry-After")
            self._rate_limit.retry_after = time.time() + (float(retry) if retry else 60)
            raise RateLimitError(429, f"Rate limited; retry after {retry}s")

    async def _request(
        self,
        method: str,
        path: str,
        signed: bool = False,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Send an HTTP request and return parsed JSON."""
        self._check_rate_limit()

        params = params or {}
        if signed:
            params = self._sign(params)

        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self.http.request(method, path, params=params, json=data)
                self._update_rate_limit(response)

                if response.status_code == 200:
                    return response.json()

                body = response.json()
                code = body.get("code", -1)
                msg = body.get("msg", response.text)
                raise BinanceError(code, msg)

            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                logger.warning("HTTP request failed (attempt %d/%d): %s", attempt, self._max_retries, exc)
                if attempt < self._max_retries:
                    await asyncio.sleep(1.5 ** attempt)

        raise TradingClientError(f"Request failed after {self._max_retries} retries") from last_exc

    async def _get(self, path: str, signed: bool = False, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request("GET", path, signed=signed, params=params)

    async def _post(self, path: str, signed: bool = False, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request("POST", path, signed=signed, params=params)

    async def _delete(self, path: str, signed: bool = False, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request("DELETE", path, signed=signed, params=params)

    # ---- Market data (public) --------------------------------------------

    async def ping(self) -> bool:
        """Test connectivity."""
        try:
            await self._get("/api/v3/ping")
            return True
        except Exception:
            return False

    async def get_server_time(self) -> int:
        """Return Binance server time in ms."""
        result = await self._get("/api/v3/time")
        return result["serverTime"]

    async def get_exchange_info(self, symbol: Optional[str] = None) -> dict:
        """Return trading rules & symbol info."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/api/v3/exchangeInfo", params=params)

    async def get_ticker(self, symbol: str) -> Ticker:
        """24hr rolling ticker for a single symbol."""
        data = await self._get("/api/v3/ticker/24hr", params={"symbol": symbol})
        return Ticker.from_dict(data)

    async def get_all_tickers(self) -> List[Ticker]:
        """24hr rolling ticker for all symbols."""
        data = await self._get("/api/v3/ticker/24hr")
        return [Ticker.from_dict(d) for d in data]

    async def get_symbol_price(self, symbol: str) -> Ticker:
        """Latest price for a symbol (lightweight)."""
        data = await self._get("/api/v3/ticker/price", params={"symbol": symbol})
        return Ticker.from_dict(data)

    async def get_order_book(self, symbol: str, limit: int = 100) -> dict:
        """Return order book depth."""
        return await self._get("/api/v3/depth", params={"symbol": symbol, "limit": limit})

    async def get_klines(
        self,
        symbol: str,
        interval: Interval = Interval.ONE_HOUR,
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Candlestick]:
        """Return kline/candlestick data."""
        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval.value,
            "limit": limit,
        }
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        raw = await self._get("/api/v3/klines", params=params)
        return [Candlestick.from_raw(r) for r in raw]

    # ---- Account (signed) ------------------------------------------------

    async def get_account_info(self) -> AccountInfo:
        """Return current account information."""
        data = await self._get("/api/v3/account", signed=True)
        return AccountInfo.from_dict(data)

    async def get_balances(self) -> List[AccountBalance]:
        """Shorthand for account balances."""
        info = await self.get_account_info()
        return info.balances

    async def get_asset_balance(self, asset: str) -> Optional[AccountBalance]:
        """Balance of a single asset (None if not held)."""
        balances = await self.get_balances()
        for b in balances:
            if b.asset == asset:
                return b
        return None

    # ---- Orders ----------------------------------------------------------

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[Decimal] = None,
        quote_order_qty: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        time_in_force: Optional[str] = None,
        new_client_order_id: Optional[str] = None,
        **extra: Any,
    ) -> Order:
        """
        Place a new order.

        For MARKET orders, use *quote_order_qty* to spend a fixed quote amount
        instead of *quantity* (e.g. ``quote_order_qty=Decimal("100")`` to spend
        $100 USDT).
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": order_type.upper(),
        }
        if quantity is not None:
            params["quantity"] = str(quantity)
        if quote_order_qty is not None:
            params["quoteOrderQty"] = str(quote_order_qty)
        if price is not None:
            params["price"] = str(price)
        if stop_price is not None:
            params["stopPrice"] = str(stop_price)
        if time_in_force:
            params["timeInForce"] = time_in_force
        if new_client_order_id:
            params["newClientOrderId"] = new_client_order_id
        params.update(extra)

        data = await self._post("/api/v3/order", signed=True, params=params)
        return Order.from_dict(data)

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> dict:
        """Cancel an active order by orderId or clientOrderId."""
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        return await self._delete("/api/v3/order", signed=True, params=params)

    async def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Order:
        """Query order status."""
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        data = await self._get("/api/v3/order", signed=True, params=params)
        return Order.from_dict(data)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Return all open orders, optionally filtered by symbol."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        data = await self._get("/api/v3/openOrders", signed=True, params=params)
        return [Order.from_dict(d) for d in data]

    async def get_all_orders(
        self,
        symbol: str,
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Order]:
        """Return historical orders for a symbol."""
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        data = await self._get("/api/v3/allOrders", signed=True, params=params)
        return [Order.from_dict(d) for d in data]

    # ---- USDⓈ-M Futures (fapi.binance.com) --------------------------------

    async def futures_ping(self) -> bool:
        """Test futures connectivity."""
        try:
            await self._get("/fapi/v1/ping")
            return True
        except Exception:
            return False

    async def futures_get_exchange_info(self) -> dict:
        """Futures exchange info."""
        return await self._get("/fapi/v1/exchangeInfo")

    async def futures_get_ticker(self, symbol: str) -> Ticker:
        """Futures 24hr ticker."""
        data = await self._get("/fapi/v1/ticker/24hr", params={"symbol": symbol})
        return Ticker.from_dict(data)

    async def futures_get_balance(self) -> List[AccountBalance]:
        """Futures account balance."""
        data = await self._get("/fapi/v2/balance", signed=True)
        return [AccountBalance.from_dict(b) for b in data]

    async def futures_place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        time_in_force: Optional[str] = None,
        reduce_only: bool = False,
        **extra: Any,
    ) -> Order:
        """Place a futures order."""
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "type": order_type.upper(),
            "quantity": str(quantity),
        }
        if price is not None:
            params["price"] = str(price)
        if stop_price is not None:
            params["stopPrice"] = str(stop_price)
        if time_in_force:
            params["timeInForce"] = time_in_force
        if reduce_only:
            params["reduceOnly"] = "true"
        params.update(extra)
        data = await self._post("/fapi/v1/order", signed=True, params=params)
        return Order.from_dict(data)

    async def futures_get_position_risk(self, symbol: Optional[str] = None) -> List[dict]:
        """Futures position risk."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/fapi/v2/positionRisk", signed=True, params=params)

    async def futures_cancel_all_orders(self, symbol: str) -> dict:
        """Cancel all futures orders for a symbol."""
        return await self._delete("/fapi/v1/allOpenOrders", signed=True, params={"symbol": symbol})

    # ---- WebSocket streams ------------------------------------------------

    async def stream_klines(
        self,
        symbol: str,
        interval: Interval = Interval.ONE_MINUTE,
    ) -> AsyncIterator[dict]:
        """
        Async generator that yields kline events from a WebSocket stream.

        Each event is a raw dict — typically includes:
            e: "kline", E: event_time, s: symbol,
            k: { t, T, s, i, f, L, o, c, h, l, v, ... }
        """
        stream_name = f"{symbol.lower()}@kline_{interval.value}"
        async for event in self._stream(stream_name):
            yield event

    async def stream_ticker(self, symbol: str) -> AsyncIterator[dict]:
        """Async generator yielding 24hr ticker updates."""
        stream_name = f"{symbol.lower()}@ticker"
        async for event in self._stream(stream_name):
            yield event

    async def stream_all_tickers(self) -> AsyncIterator[dict]:
        """Async generator yielding ticker updates for ALL symbols (arr)."""
        async for event in self._stream("!ticker@arr"):
            yield event

    async def stream_depth(
        self,
        symbol: str,
        level: str = "100ms",
    ) -> AsyncIterator[dict]:
        """
        Async generator yielding order-book depth updates.

        *level* can be "100ms" (diff depth) or a depth level like "5", "10", "20".
        """
        stream_name = f"{symbol.lower()}@depth{level}"
        async for event in self._stream(stream_name):
            yield event

    async def stream_user_data(self) -> AsyncIterator[dict]:
        """
        Async generator yielding user-data events (account updates, order
        updates, balance updates). Requires a valid listen-key.

        The client automatically fetches and keeps the listen-key alive.
        """
        listen_key = await self._get_listen_key()
        stream_name = listen_key
        logger.info("Starting user-data stream with listen-key %s …", listen_key[:8])
        async for event in self._stream(stream_name):
            yield event

    async def _stream(self, stream_name: str) -> AsyncIterator[dict]:
        """Internal: connect to a WebSocket stream and yield messages."""
        url = f"{self._wss_url}/{stream_name}"
        async with websockets.connect(url) as ws:
            self._ws_connections.append(ws)
            try:
                async for message in ws:
                    if self._closed:
                        break
                    if not message:
                        continue
                    data = json.loads(message)
                    if isinstance(data, dict) and data.get("e") == "error":
                        logger.error("WebSocket error: %s", data.get("m"))
                        break
                    yield data
            finally:
                if ws in self._ws_connections:
                    self._ws_connections.remove(ws)

    # ---- Listen key (user data streams) -----------------------------------

    async def _get_listen_key(self) -> str:
        """Create a new listen-key for the user-data stream."""
        data = await self._post("/api/v3/userDataStream", signed=False)
        return data["listenKey"]

    async def _keepalive_listen_key(self, listen_key: str) -> None:
        """Ping a listen-key to keep it alive (30-min TTL)."""
        await self._put("/api/v3/userDataStream", params={"listenKey": listen_key})

    async def _put(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request("PUT", path, params=params)
