from dataclasses import dataclass
from os import getenv
from yaml import safe_load


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
class Configuration:
    dump_interval: int
    dump_directory: str
    state_file: str
    market_feed: MarketFeedConfiguration
    crypto_feed: CryptoFeedConfiguration


def get_configuration():
    configuration_file = getenv("POLYJUICE_CONFIGURATION_FILE")
    if not configuration_file:
        raise Exception("Configuration file not specified")
    with open(configuration_file) as file:
        configuration = Configuration(**safe_load(file))
    configuration.market_feed = MarketFeedConfiguration(**configuration.market_feed)
    configuration.crypto_feed = CryptoFeedConfiguration(**configuration.crypto_feed)
    return configuration
