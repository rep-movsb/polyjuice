from dataclasses import dataclass

from polyjuice.configurations.base import Configuration


@dataclass
class MarketFeedConfiguration:
    websocket_url: str
    websocket_ping_interval: str
    slug_filters: list[str]


@dataclass
class CryptoFeedConfiguration:
    gamma_url: str
    rtds_websocket_url: str
    rtds_websocket_ping_interval: int
    watched_symbols: list[str]
    watched_ranges: list[str]


@dataclass
class CollectorConfiguration(Configuration("POLYJUICE_COLLECTOR_CONFIGURATION_FILE")):
    dump_interval: int
    dump_directory: str
    state_file: str
    market_feed: MarketFeedConfiguration
    crypto_feed: CryptoFeedConfiguration
