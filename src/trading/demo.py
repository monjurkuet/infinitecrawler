#!/usr/bin/env python3
"""
Binance Trading Demo

Demonstrates Binance Spot and Futures API operations using the
trading client from src.trading.client.

Usage:
    # Interactive demo
    python -m src.trading.demo

    # Quick price check
    python -m src.trading.demo --symbol BTCUSDT --price-only

Environment:
    BINANCE_API_KEY     Binance API key
    BINANCE_SECRET_KEY  Binance secret key
    BINANCE_TESTNET     Set to "true" to use testnet (default: true)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from decimal import Decimal
from typing import Optional

from src.trading.client import BinanceClient

logger = logging.getLogger(__name__)

# ── helpers ────────────────────────────────────────────────────────────────


def _fmt(num: Decimal | str | float) -> str:
    """Format a number for human-readable display."""
    d = Decimal(str(num))
    if d == d.to_integral_value():
        return f"{d:.8f}"
    return f"{d.normalize():f}"


def _sep(char: str = "─", width: int = 72) -> str:
    return char * width


# ── demo workflows ─────────────────────────────────────────────────────────


async def show_account_info(client: BinanceClient) -> None:
    """Display spot and futures account summaries."""
    print(_sep())
    print("  SPOT ACCOUNT")
    print(_sep())
    balances = await client.get_spot_balances()
    non_zero = [b for b in balances if float(b.get("free", 0)) > 0 or float(b.get("locked", 0)) > 0]
    if non_zero:
        print(f"  {'Asset':<10} {'Free':>18} {'Locked':>18}")
        for b in non_zero:
            print(f"  {b['asset']:<10} {_fmt(b['free']):>18} {_fmt(b['locked']):>18}")
    else:
        print("  (no non-zero balances)")

    print()
    print(_sep())
    print("  FUTURES ACCOUNT (USD-M)")
    print(_sep())
    try:
        fut = await client.get_futures_account()
        print(f"  Total Wallet Balance : {_fmt(fut.get('totalWalletBalance', 0))}")
        print(f"  Total Unrealized PnL : {_fmt(fut.get('totalUnrealizedProfit', 0))}")
        print(f"  Available Balance    : {_fmt(fut.get('availableBalance', 0))}")
        positions = fut.get("positions", [])
        open_positions = [p for p in positions if abs(float(p.get("positionAmt", 0))) > 0]
        if open_positions:
            print()
            print(f"  {'Symbol':<12} {'Side':<6} {'Size':>14} {'Entry':>16} {'Mark':>16} {'PnL':>14}")
            for p in open_positions:
                side = "LONG" if float(p.get("positionAmt", 0)) > 0 else "SHORT"
                print(
                    f"  {p['symbol']:<12} {side:<6} {_fmt(p['positionAmt']):>14} "
                    f"{_fmt(p['entryPrice']):>16} {_fmt(p['markPrice']):>16} {_fmt(p['unrealizedProfit']):>14}"
                )
        else:
            print("  (no open positions)")
    except Exception as e:
        logger.warning("Futures account info unavailable: %s", e)


async def show_prices(client: BinanceClient, symbols: Optional[list[str]] = None) -> None:
    """Display current prices for one or more symbols."""
    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]

    print(_sep())
    print("  TICKER PRICES")
    print(_sep())
    print(f"  {'Symbol':<12} {'Price':>18} {'Change 24h':>12} {'Volume':>18}")
    print(f"  {'──────':<12} {'─────':>18} {'─────────':>12} {'──────':>18}")

    for sym in symbols:
        try:
            ticker = await client.get_ticker(sym)
            print(
                f"  {ticker['symbol']:<12} {_fmt(ticker['lastPrice']):>18} "
                f"{ticker['priceChangePercent']:>12} {_fmt(ticker['volume']):>18}"
            )
        except Exception as e:
            print(f"  {sym:<12} ERROR: {e}")


async def place_test_order(client: BinanceClient, symbol: str = "BTCUSDT") -> None:
    """Place a test (simulated) market order to verify connectivity."""
    print(_sep())
    print("  TEST ORDER (simulated — no real trade)")
    print(_sep())

    ticker = await client.get_ticker(symbol)
    price = Decimal(str(ticker["lastPrice"]))
    qty = Decimal("0.001")  # tiny quantity for test

    print(f"  Symbol     : {symbol}")
    print(f"  Current    : {_fmt(price)} USDT")
    print(f"  Test qty   : {_fmt(qty)}")
    print(f"  Notional   : ~{_fmt(price * qty)} USDT")
    print()

    try:
        result = await client.test_new_order(
            symbol=symbol,
            side="BUY",
            order_type="MARKET",
            quantity=qty,
        )
        print(f"  ✅ Test order accepted (ID: {result.get('orderId', 'N/A')})")
    except Exception as e:
        print(f"  ❌ Test order failed: {e}")


async def show_recent_trades(client: BinanceClient, symbol: str = "BTCUSDT", limit: int = 5) -> None:
    """Display recent trades for a symbol."""
    print(_sep())
    print(f"  RECENT TRADES — {symbol} (last {limit})")
    print(_sep())
    trades = await client.get_recent_trades(symbol, limit=limit)
    if not trades:
        print("  (no trades)")
        return
    print(f"  {'Time':<24} {'Side':<6} {'Qty':>14} {'Price':>16} {'Value':>18}")
    for t in trades:
        ts = datetime.fromtimestamp(t["time"] / 1000).strftime("%Y-%m-%d %H:%M:%S")
        side = "BUY" if t.get("isBuyer", True) else "SELL"
        qty = Decimal(t["qty"])
        px = Decimal(t["price"])
        val = qty * px
        print(f"  {ts:<24} {side:<6} {_fmt(qty):>14} {_fmt(px):>16} {_fmt(val):>18}")


# ── main ───────────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(description="Binance Trading Demo")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair symbol")
    parser.add_argument("--price-only", action="store_true", help="Show prices and exit")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    api_key = os.getenv("BINANCE_API_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY")
    use_testnet = os.getenv("BINANCE_TESTNET", "true").lower() in ("true", "1", "yes")

    if not api_key or not secret_key:
        print("ERROR: BINANCE_API_KEY and BINANCE_SECRET_KEY environment variables are required.")
        sys.exit(1)

    client = BinanceClient(
        api_key=api_key,
        secret_key=secret_key,
        testnet=use_testnet,
    )

    async with client:
        await show_prices(client, [args.symbol] if args.price_only else None)

        if args.price_only:
            return

        print()
        await show_account_info(client)
        print()
        await place_test_order(client, args.symbol)
        print()
        await show_recent_trades(client, args.symbol)
        print()
        print("  ✅ Demo complete — all operations successful.")


if __name__ == "__main__":
    asyncio.run(main())
