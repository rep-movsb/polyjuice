from aiohttp import ClientSession
from asyncio import TaskGroup
from datetime import datetime, timedelta
from itertools import product
from json import JSONDecodeError, loads, dumps
from logging import warning
from websockets import connect
from websockets.asyncio.client import ClientConnection

from polyjuice.collectorinterface import CollectorInterface
from polyjuice.configurations.collector import (
    CollectorConfiguration,
    CryptoFeedConfiguration,
)
from polyjuice.feed import Feed
from polyjuice.models import Asset, Market


def conf() -> CryptoFeedConfiguration:
    return CollectorConfiguration.get().crypto_feed


def compute_crypto_slug(prefix, ts, interval):
    if interval == "5m":
        rounded = (ts // 300) * 300
    elif interval == "15m":
        rounded = (ts // 900) * 900
    elif interval == "1h":
        rounded = (ts // 3600) * 3600
    elif interval == "4h":
        rounded = (ts // (3600 * 4)) * (3600 * 4)
    return (f"{prefix}-updown-{interval}-{rounded}", rounded)


class CryptoFeed(Feed):

    async def initialize(self, exchange: CollectorInterface):
        self.exchange = exchange

    async def fetch_loop(self):
        async with connect(
            conf().rtds_websocket_url,
            ping_interval=conf().rtds_websocket_ping_interval,
            ping_timeout=None,
        ) as connection:
            subscribe_message = {
                "action": "subscribe",
                "subscriptions": [{"topic": "crypto_prices", "type": "update"}],
            }
            await connection.send(dumps(subscribe_message))

            async with TaskGroup() as task_group:
                task_group.create_task(self.subscribe_crypto_markets_24h())
                task_group.create_task(self.receive_crypto_updates_loop(connection))

    async def receive_crypto_updates_loop(self, connection: ClientConnection):
        while True:
            message = await connection.recv()
            try:
                data = loads(message)
            except JSONDecodeError:
                warning(f"Received unexpected message: {message}")
                continue
            self.exchange.log_event(data)
            print(
                f"CRYPTO_PRICE     {data['payload']['symbol']:8} {data['payload']['value']:>10}"
            )

    async def get_crypto_market(self, slug: str) -> Market:
        async with ClientSession() as session:
            async with session.get(f"{conf().gamma_url}/{slug}") as response:
                response.raise_for_status()
                data = await response.json()

        markets = data["markets"]
        if len(markets) != 1:
            raise Exception(f"Found {len(markets)} matching markets for {slug}")
        market = markets[0]

        return Market(
            id=market["id"],
            conditionId=market["conditionId"],
            slug=market["slug"],
            assets=[
                Asset(id, loads(market["outcomes"])[i])
                for (i, id) in enumerate(loads(market["clobTokenIds"]))
            ],
        )

    async def subscribe_crypto_markets_24h(self):
        now = datetime.now().replace(microsecond=0)
        range_24h = [
            int((now + timedelta(minutes=m)).timestamp()) for m in range(0, 24 * 60)
        ]
        watched_crypto_slugs = set(
            compute_crypto_slug(c, ts, i)
            for (c, ts, i) in product(
                conf().watched_symbols,
                range_24h,
                conf().watched_ranges,
            )
        )
        for slug, _ in sorted(watched_crypto_slugs, key=lambda e: e[1]):
            if not any(market.slug == slug for market in self.exchange.get_markets()):
                try:
                    market = await self.get_crypto_market(slug)
                    await self.exchange.subscribe_to_market(market)
                except Exception as ex:
                    warning(f"Failed to subscribe to market {slug}: {str(ex)}")
