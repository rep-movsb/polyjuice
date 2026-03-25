from dataclasses import dataclass
from polars import Boolean, Float32, Float64
from re import match
from typing import Any

from polyjuice.index import Index
from polyjuice.states.market import MarketState
from polyjuice.state import StateChange


@dataclass
class MarketRange:
    start: float
    end: float
    currently_in_range: bool | None = None

    def from_slug(slug: str):
        match_object = match(r"^.*-updown-(5m|15m|1h|4h)-([0-9]+)$", slug)
        if not match_object:
            return None
        interval, start_timestamp = match_object.groups(1)
        if interval == "5m":
            duration = 300
        elif interval == "15m":
            duration = 900
        elif interval == "1h":
            duration = 3600
        elif interval == "4h":
            duration = 3600 * 4
        return MarketRange(float(start_timestamp), float(start_timestamp) + duration)


class CryptoMarketState(MarketState):
    index: Index
    target_symbol: str
    market_ranges: dict[str, MarketRange]
    value: float | None = None

    def __init__(self, index: Index, target_symbol: str):
        super().__init__()
        self.index = index
        self.target_symbol = target_symbol
        self.market_ranges = dict()
        self.value = None

    def schema(self):
        return super().schema() | {
            f"{self.target_symbol}_value": Float32,
            "range_start": Float64,
            "range_end": Float64,
            "in_range": Boolean,
        }

    def process_event(self, timestamp, event):
        if (
            "topic" in event
            and event["topic"] == "crypto_prices"
            and event["payload"]["symbol"] == self.target_symbol
        ):
            value = float(event["payload"]["value"])

            def update_value(value=value):
                self.value = value

            yield StateChange(timestamp, update_value)
        else:
            yield from super().process_event(timestamp, event)

    def process_market_event(
        self, timestamp: float, market_id: str, event: dict[str, Any]
    ):
        if market_id in self.market_ranges:
            range = self.market_ranges[market_id]
        elif market_id in self.index.markets:
            range = MarketRange.from_slug(self.index.markets[market_id].slug)
            self.market_ranges[market_id] = range
        else:
            range = None
        if range is not None:
            in_range = timestamp >= range.start and timestamp < range.end

            def update_in_range(range=range, in_range=in_range):
                range.currently_in_range = in_range

            yield StateChange(timestamp, update_in_range)
        yield from super().process_market_event(timestamp, market_id, event)

    def enrich(self, timestamp, stats, market_id: str, market, asset_id, asset):
        if market_id in self.market_ranges:
            range_start = self.market_ranges[market_id].start
            range_end = self.market_ranges[market_id].end
            in_range = self.market_ranges[market_id].currently_in_range
        else:
            range_start = None
            range_end = None
            in_range = None
        return super().enrich(timestamp, stats, market_id, market, asset_id, asset) | {
            f"{self.target_symbol}_value": self.value,
            "range_start": range_start,
            "range_end": range_end,
            "in_range": in_range,
        }
