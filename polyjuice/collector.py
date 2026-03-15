from asyncio import gather, run, sleep
from dataclasses import asdict
from json import JSONDecodeError, dump, dumps, load
from logging import error, info, warning
from time import time
from typing import Any, Type, Iterable

from polyjuice.collectorinterface import CollectorInterface
from polyjuice.configuration import get_configuration
from polyjuice.feed import Feed
from polyjuice.feeds.market import MarketSubscriber, market_connect
from polyjuice.models import Market

configuration = get_configuration()


class CollectorExchange(CollectorInterface):

    subscriber: MarketSubscriber
    subscribed_markets: dict[id, Market]
    asset_to_outcome_map: dict[str, str]
    events: list[dict[str, Any]]
    dump_start_timestamp: float

    def __init__(self, subscriber: MarketSubscriber):
        self.subscriber = subscriber
        self.subscribed_markets = dict()
        self.asset_to_outcome_map = dict()
        self.events = list()
        self.dump_start_timestamp = time()

    async def subscribe_to_market(self, market: Market):
        await self.subscriber.subscribe_to_assets([asset.id for asset in market.assets])
        self.subscribed_markets[market.conditionId] = market
        for asset in market.assets:
            self.asset_to_outcome_map[asset.id] = asset.outcome
        info(f"Subscribed to market {market.id} {market.slug}")

    def get_market_condition_ids(self) -> list[str]:
        return list(self.subscribed_markets.keys())

    def get_market_from_condition_id(self, condition_id: str) -> Market | None:
        if condition_id not in self.subscribed_markets:
            return None
        return self.subscribed_markets[condition_id]

    def get_markets(self) -> Iterable[Market]:
        return self.subscribed_markets.values()

    def get_outcome_for_asset(self, asset_id: str) -> str:
        return self.asset_to_outcome_map[asset_id]

    def log_event(self, event: dict):
        self.events.append(event | {"local_timestamp": time()})

    def get_events(self) -> list[dict[str, Any]]:
        return self.events

    async def recover_state(self):
        try:
            with open(configuration.state_file) as file:
                for id, market in load(file).items():
                    await self.subscribe_to_market(Market.from_dict(**market))
        except OSError:
            info("Could not open state file")
        except JSONDecodeError:
            warning("Could not recover state")
        except Exception as ex:
            print(str(ex))
            raise
        info(f"{len(self.subscribed_markets)} markets recovered")

    def save_state(self):
        with open(configuration.state_file, "w") as file:
            dump(
                {
                    market.id: asdict(market)
                    for market in self.subscribed_markets.values()
                },
                file,
            )
        info(f"Saved state")

    def dump_events(self, suffix=""):
        filename = (
            f"{configuration.dump_directory}/{self.dump_start_timestamp}{suffix}.jsonl"
        )
        with open(filename, "w") as file:
            for event in self.events:
                file.write(f"{dumps(event)}\n")
        info(f"Dumped {len(self.events)} events")
        self.events = []
        self.dump_start_timestamp = time()


class Collector:

    def __init__(self, *FeedClasses: Type[Feed]):
        self.FeedClasses = FeedClasses

    def run_sync(self):
        run(self.run())

    async def run(self):
        try:
            async with market_connect() as market_connection:
                subscriber = MarketSubscriber(market_connection)
                exchange = CollectorExchange(subscriber)
                feeds = [FeedClass() for FeedClass in self.FeedClasses]
                for feed in feeds:
                    await feed.initialize(exchange)
                await exchange.recover_state()
                info(f"Started collector at {time()}")
                await gather(
                    *[
                        self.dump_data_loop(exchange),
                        *(feed.fetch_loop() for feed in feeds),
                    ]
                )
        except Exception as ex:
            error(f"Uncaught exception: {str(ex)}")
            exchange.dump_events(".incomplete")
            raise

    async def dump_data_loop(self, exchange: CollectorExchange):
        while True:
            await sleep(configuration.dump_interval)
            exchange.save_state()
            exchange.dump_events()


if __name__ == "__main__":

    from logging import basicConfig, INFO
    from polyjuice.feeds.crypto import CryptoFeed
    from polyjuice.feeds.market import MarketFeed

    basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    collector = Collector(MarketFeed, CryptoFeed)
    collector.run_sync()
