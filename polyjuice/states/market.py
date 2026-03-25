from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from json import loads
from logging import debug
from polars import Date, Float32, Float64, String, UInt32
from typing import Any

from polyjuice.data.dataset import DatasetOrderedStreamReader
from polyjuice.state import AggregationStatistics, State, StateChange


@dataclass
class Level:
    size: float = 0
    has_value: bool = True


@dataclass
class Asset:
    last_trade_price: str | None = None
    last_trade_size: float | None = None
    last_trade_is_bid: bool | None = None
    best_bid: str | None = None
    best_ask: str | None = None
    bid_levels: dict[int, Level] = field(
        default_factory=lambda: [Level(has_value=False)] * 99
    )
    ask_levels: dict[int, Level] = field(
        default_factory=lambda: [Level(has_value=False)] * 99
    )
    book_resets: int = 0


@dataclass
class Market:
    assets: dict[str, Asset] = field(default_factory=lambda: defaultdict(Asset))


def level_as_int(level):
    return int(float(level) * 100) - 1


def compute_imbalance(bid_levels: list[Level], ask_levels: list[Level]):
    bid_volume = sum(
        bid_levels[n].size if bid_levels[n].has_value else 0 for n in range(99)
    )
    ask_volume = sum(
        ask_levels[n].size if ask_levels[n].has_value else 0 for n in range(99)
    )
    if bid_volume + ask_volume == 0:
        return 0
    return (bid_volume - ask_volume) / (bid_volume + ask_volume)


def get_best_bid_and_ask(bid_levels: list[Level], ask_levels: list[Level]):
    best_bid = 100
    for n in range(99, 0, -1):
        if bid_levels[n - 1].has_value and bid_levels[n - 1].size > 0:
            best_bid = n
            break
    best_ask = 0
    for n in range(1, 100):
        if ask_levels[n - 1].has_value and ask_levels[n - 1].size > 0:
            best_ask = n
            break
    return best_bid, best_ask


def generate_levels(bid_levels: list[Level], ask_levels: list[Level]):
    return {
        f"bid_{n}": bid_levels[n - 1].size if bid_levels[n - 1].has_value else None
        for n in range(1, 100)
    } | {
        f"ask_{n}": ask_levels[n - 1].size if ask_levels[n - 1].has_value else None
        for n in range(1, 100)
    }


@dataclass
class MarketState(State):
    markets: dict[str, Market] = field(default_factory=lambda: defaultdict(Market))

    def partitioning_fields(self):
        return ["date"]

    def schema(self):
        return (
            {
                "date": Date,
                "timestamp": Float64,
                "changes": UInt32,
                "mean_delay": Float32,
                "market_id": String,
                "asset_id": String,
                "book_resets": UInt32,
                "last_trade_price": Float32,
                "last_trade_size": Float32,
                "last_trade_id_bid": String,
                "best_bid": Float32,
                "best_ask": Float32,
                "midprice": Float32,
                "spread": Float32,
                "imbalance": Float32,
            }
            | {f"bid_{n}": Float32 for n in range(1, 100)}
            | {f"ask_{n}": Float32 for n in range(1, 100)}
        )

    async def process_stream(self, reader: DatasetOrderedStreamReader):
        first_timestamp_output = False
        for line in reader.read():
            event = loads(line)
            timestamp = float(event["local_timestamp"])
            if not first_timestamp_output:
                yield timestamp
                first_timestamp_output = True
            for change in self.process_event(timestamp, event):
                yield change

    def process_event(self, timestamp: float, event: dict[str, Any]):
        if not "market" in event:
            return
        yield from self.process_market_event(timestamp, event["market"], event)

    def process_market_event(
        self, timestamp: float, market_id: str, event: dict[str, Any]
    ):
        if event["event_type"] == "book":
            asset = self.markets[market_id].assets[event["asset_id"]]
            bids = event["bids"]
            asks = event["asks"]

            def initialize_book(asset=asset, bids=bids, asks=asks):
                asset.book_resets += 1
                asset.bid_levels = [Level(has_value=False)] * 99
                for bid in bids:
                    asset.bid_levels[level_as_int(bid["price"])] = Level(
                        float(bid["size"])
                    )
                asset.ask_levels = [Level(has_value=False)] * 99
                for ask in asks:
                    asset.ask_levels[level_as_int(ask["price"])] = Level(
                        float(ask["size"])
                    )

            yield StateChange(timestamp, initialize_book)
        elif event["event_type"] == "price_change":
            price_changes = event["price_changes"]
            assets = self.markets[market_id].assets

            def apply_price_changes(price_changes=price_changes, assets=assets):
                for pc in price_changes:
                    asset: Asset = assets[pc["asset_id"]]
                    levels = (
                        asset.bid_levels if pc["side"] == "BUY" else asset.ask_levels
                    )
                    levels[level_as_int(pc["price"])] = Level(float(pc["size"]))
                    asset.best_bid = pc["best_bid"]
                    asset.best_ask = pc["best_ask"]

            yield StateChange(timestamp, apply_price_changes)
        elif event["event_type"] == "last_trade_price":
            asset = self.markets[market_id].assets[event["asset_id"]]
            price = event["price"]
            size = event["size"]
            side = event["side"]

            def apply_last_trade_price(asset=asset, price=price, size=size, side=side):
                asset.last_trade_price = price
                asset.last_trade_size = size
                asset.last_trade_is_bid = side == "BUY"

            yield StateChange(timestamp, apply_last_trade_price)
        elif event["event_type"] == "best_bid_ask":
            pass
        else:
            debug(event)

    def export(self, timestamp: int, stats: AggregationStatistics):
        enriched = []
        for market_id, market in self.markets.items():
            for asset_id, asset in market.assets.items():
                enriched.append(
                    self.enrich(timestamp, stats, market_id, market, asset_id, asset)
                )
        return enriched

    def enrich(
        self,
        timestamp: float,
        stats: AggregationStatistics,
        market_id: str,
        market: Market,
        asset_id: str,
        asset: Asset,
    ):
        imbalance = compute_imbalance(asset.bid_levels, asset.ask_levels)
        best_bid, best_ask = get_best_bid_and_ask(asset.bid_levels, asset.ask_levels)
        midprice = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        levels = generate_levels(asset.bid_levels, asset.ask_levels)
        return {
            "date": datetime.fromtimestamp(timestamp / 1000).date(),
            "timestamp": timestamp,
            "changes": stats.changes,
            "mean_delay": stats.mean_delay,
            "market_id": market_id,
            "asset_id": asset_id,
            "book_resets": asset.book_resets,
            "last_trade_price": float(asset.last_trade_price or 0),
            "last_trade_size": float(asset.last_trade_size or 0),
            "last_trade_id_bid": "BUY" if asset.last_trade_is_bid else "SELL",
            "best_bid": best_bid,
            "best_ask": best_ask,
            "midprice": midprice,
            "spread": spread,
            "imbalance": imbalance,
        } | levels
