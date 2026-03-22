from collections import defaultdict
from dataclasses import asdict, dataclass, field
from json import dump, load
from lzma import open as lzma_open
from typing import Any


@dataclass
class MarketInfo:
    id: int | None = None
    conditionId: str = None
    slug: str | None = None
    first_seen: float | None = None
    last_seen: float | None = None
    stream_groups: set[str] = field(default_factory=set)


@dataclass
class Index:
    markets: dict[str, MarketInfo] = field(
        default_factory=lambda: defaultdict(MarketInfo)
    )

    def load(filename: str):
        with lzma_open(filename, "rt") as index_file:
            index_dict = load(index_file)
        markets: dict[str, MarketInfo] = {
            market["conditionId"]: MarketInfo(**market)
            for market in index_dict["markets"]
        }
        for market_info in markets.values():
            market_info.stream_groups = set(market_info.stream_groups)
        return Index(defaultdict(MarketInfo, markets))

    def save(self, filename: str):
        with lzma_open(filename, "wt") as index_file:
            dump(
                {
                    "markets": [
                        asdict(market_info)
                        | {"stream_groups": list(market_info.stream_groups)}
                        for market_info in self.markets.values()
                    ]
                },
                index_file,
            )

    def process_event(self, event: dict[str, Any], stream_group: str):
        if "event_type" in event and "market" in event:
            market_info: MarketInfo = self.markets[event["market"]]
            market_info.conditionId = event["market"]
            if event["event_type"] == "new_market":
                market_info.id = event["id"]
                market_info.conditionId = event["market"]
                market_info.slug = event["slug"]
            if (
                market_info.first_seen is None
                or event["local_timestamp"] < market_info.first_seen
            ):
                market_info.first_seen = event["local_timestamp"]
            if (
                market_info.last_seen is None
                or event["local_timestamp"] > market_info.last_seen
            ):
                market_info.last_seen = event["local_timestamp"]
            market_info.stream_groups |= {stream_group}
            return 1
        return 0

    def update(self, outer_index: "Index"):
        for id, market_info in outer_index.markets.items():
            if id in self.markets:
                market_info.first_seen = min(
                    market_info.first_seen, self.markets[id].first_seen
                )
                market_info.last_seen = max(
                    market_info.last_seen, self.markets[id].last_seen
                )
                market_info.stream_groups |= self.markets[id].stream_groups
            self.markets[id] = market_info
