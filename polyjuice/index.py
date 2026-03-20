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
    stream_groups: set[str] | None = None


@dataclass
class Index:
    markets: dict[str, MarketInfo] = field(
        default_factory=lambda: defaultdict(MarketInfo)
    )
    files: set[str] = field(default_factory=set)


def load_index(index_filename: str):
    with lzma_open(index_filename, "rt") as index_file:
        indexes = load(index_file)
        market_index: dict[str, MarketInfo] = {
            market["conditionId"]: MarketInfo(**market) for market in indexes["markets"]
        }
        file_index = set(indexes["files"])
    for market_info in market_index.values():
        market_info.stream_groups = set(market_info.stream_groups)
    return Index(market_index, file_index)


def save_index(index_filename: str, index: Index):
    with lzma_open(index_filename, "wt") as index_file:
        dump(
            {
                "markets": [
                    asdict(market_info)
                    | {"stream_groups": list(market_info.stream_groups)}
                    for market_info in index.markets.values()
                ],
                "files": list(index.files),
            },
            index_file,
        )


def process_event(index: Index, file: str, event: dict[str, Any], stream_group: str):
    if "event_type" in event and "market" in event:
        market_info: MarketInfo = index.markets[event["market"]]
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
        market_info.stream_groups = set([stream_group])
        index.files.add(file)
        return 1
    return 0


def merge_indexes(index: Index, outer_index: Index):
    for id, market_info in outer_index.markets.items():
        if id in index.markets:
            market_info.first_seen = min(
                market_info.first_seen, index.markets[id].first_seen
            )
            market_info.last_seen = max(
                market_info.last_seen, index.markets[id].last_seen
            )
            market_info.stream_groups |= index.markets[id].stream_groups
        index.markets[id] = market_info
    index.files |= outer_index.files
