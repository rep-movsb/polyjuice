from asyncio import gather, run, sleep
from dataclasses import asdict
from json import JSONDecodeError, dump, dumps, load
from logging import error, info, warning
from multiprocessing import Process, Queue
from time import time
from typing import Any, Type, Iterable

from polyjuice.collectorinterface import CollectorInterface
from polyjuice.configurations.collector import CollectorConfiguration
from polyjuice.feed import Feed
from polyjuice.feeds.market import MarketSubscriber, market_connect
from polyjuice.models import Market


def conf() -> CollectorConfiguration:
    return CollectorConfiguration.get()


class WriterProcessException(Exception): ...


class Collector(CollectorInterface):

    FeedClasses: list[type[Feed]]
    subscriber: MarketSubscriber
    subscribed_markets: dict[id, Market]
    asset_to_outcome_map: dict[str, str]
    events: list[dict[str, Any]]
    writer_process: Process
    write_queue: Queue

    def __init__(self, *FeedClasses: Type[Feed]):
        self.feeds = [FeedClass() for FeedClass in FeedClasses]
        self.subscriber = None
        self.subscribed_markets = dict()
        self.asset_to_outcome_map = dict()
        self.events = list()
        self.writer_process = None
        self.write_queue = Queue()

    def run_sync(self):
        run(self.run())

    async def run(self):
        try:
            self.writer_process = Process(
                target=self.write_chunks, args=(self.write_queue,)
            )
            self.writer_process.start()
            async with market_connect() as market_connection:
                self.subscriber = MarketSubscriber(market_connection)
                for feed in self.feeds:
                    await feed.initialize(self)
                await self.recover_state()
                info(f"Started collector at {time()}")
                await gather(
                    *[
                        self.dump_data_loop(),
                        self.monitor_writer_process_loop(),
                        *(feed.fetch_loop() for feed in self.feeds),
                    ]
                )
        except WriterProcessException:
            self.write_queue.cancel_join_thread()
            raise
        except Exception as ex:
            error(f"Uncaught exception: {str(ex)}")
            self.dump_events(".incomplete")
            self.write_queue.put(None)
            self.writer_process.join()
            raise

    async def dump_data_loop(self):
        while True:
            await sleep(conf().dump_interval)
            self.save_state()
            self.dump_events()

    async def monitor_writer_process_loop(self):
        while True:
            await sleep(1)
            if not self.writer_process.is_alive():
                raise WriterProcessException("Writer process is dead")

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
        self.write_queue.put(event | {"local_timestamp": time()})

    async def recover_state(self):
        try:
            with open(conf().state_file) as file:
                for market in load(file).values():
                    await self.subscribe_to_market(Market.from_dict(**market))
        except OSError:
            info("Could not open state file")
        except JSONDecodeError:
            warning("Could not recover state")
        except Exception as ex:
            error(str(ex))
            raise
        info(f"{len(self.subscribed_markets)} markets recovered")

    def save_state(self):
        with open(conf().state_file, "w") as file:
            dump(
                {
                    market.id: asdict(market)
                    for market in self.subscribed_markets.values()
                },
                file,
            )
        info(f"Saved state")

    def dump_events(self, suffix=""):
        self.write_queue.put(suffix)

    def write_chunks(self, queue: Queue):
        dump_start_timestamp = time()
        events = []
        while (item := queue.get()) is not None:
            if type(item) is str:
                filename = f"{conf().dump_directory}/{dump_start_timestamp}{item}.jsonl"
                with open(filename, "w") as file:
                    for event in events:
                        file.write(f"{dumps(event)}\n")
                info(f"Dumped {len(events)} events")
                events = []
                dump_start_timestamp = time()
            else:
                events.append(item)


if __name__ == "__main__":

    from click import Path, command, option
    from logging import basicConfig, INFO
    from polyjuice.feeds.crypto import CryptoFeed
    from polyjuice.feeds.market import MarketFeed

    basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    @command()
    @option("--configuration-file", type=Path(readable=True))
    def main(configuration_file: str | None):
        if not configuration_file:
            raise Exception("Configuration file not specified")
        with CollectorConfiguration.load(configuration_file):
            collector = Collector(MarketFeed, CryptoFeed)
            collector.run_sync()

    main()
