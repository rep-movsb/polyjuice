from contextlib import asynccontextmanager
from contextvars import ContextVar
from json import JSONDecodeError, dumps, loads
from logging import debug, info, warning
from websockets import connect
from websockets.asyncio.client import ClientConnection

from polyjuice.collectorinterface import CollectorInterface
from polyjuice.configuration import get_configuration
from polyjuice.feed import Feed
from polyjuice.models import Asset, Market

configuration = get_configuration().market_feed

global_market_connection = ContextVar("global_market_connection")


@asynccontextmanager
async def market_connect():
    async with connect(
        configuration.websocket_url,
        ping_interval=configuration.websocket_ping_interval,
        ping_timeout=None,
    ) as websocket:
        global_market_connection.set(websocket)
        yield websocket
        global_market_connection.set(None)


class MarketSubscriber:

    connection: ClientConnection

    def __init__(self, connection: connect):
        self.connection = connection

    async def subscribe_to_assets(self, assets_ids: list[str]):
        msg = {
            "operation": "subscribe",
            "assets_ids": assets_ids,
            "custom_feature_enabled": True,
        }
        await self.connection.send(dumps(msg))


class MarketFeed(Feed):

    connection: ClientConnection
    exchange: CollectorInterface

    async def initialize(self, exchange: CollectorInterface):
        self.connection = global_market_connection.get()
        self.exchange = exchange
        subscribe_message = {
            "type": "market",
            "assets_ids": [],
            "custom_feature_enabled": True,
        }
        await self.connection.send(dumps(subscribe_message))
        info("Subscribed to new_market notifications")

    async def fetch_loop(self):

        while True:
            message = await self.connection.recv()
            try:
                data = loads(message)
            except JSONDecodeError:
                if message != "PONG":
                    warning(f"Received unexpected message: {message}")
                else:
                    debug("Received PONG")
                continue

            if not isinstance(data, list):
                data = [data]

            for event in data:
                self.exchange.log_event(event)

                if event["event_type"] == "new_market":
                    if any(
                        filter in event.get("slug")
                        for filter in configuration.slug_filters
                    ):
                        if (
                            event["market"]
                            not in self.exchange.get_market_condition_ids()
                        ):
                            info(f"Found new market {event['market']} {event['slug']}")
                            market = Market(
                                id=event["id"],
                                conditionId=event["market"],
                                slug=event["slug"],
                                assets=[
                                    Asset(asset_id, event["outcomes"][i])
                                    for i, asset_id in enumerate(event["assets_ids"])
                                ],
                            )
                            await self.exchange.subscribe_to_market(market)
                    else:
                        print(f"Skipped market {event['market']} {event['slug']}")

                elif event["event_type"] == "price_change":
                    market = self.exchange.get_market_from_condition_id(event["market"])
                    for pc in event["price_changes"]:
                        outcome = self.exchange.get_outcome_for_asset(pc["asset_id"])
                        print(
                            f"PRICE_CHANGE     {market.slug[:50]:<50} {outcome[:30]:<30} {pc['side']:>4} {pc['price'][:5]:>5} {pc['size'][:10]:>10} {pc['best_bid'][:6]:>6} {pc['best_ask'][:6]:>6}"
                        )

                elif event["event_type"] == "book":
                    market = self.exchange.get_market_from_condition_id(event["market"])
                    outcome = self.exchange.get_outcome_for_asset(event["asset_id"])
                    print(
                        f"BOOK             {market.slug[:50]:<50} {outcome[:30]:<30} {len(event['bids']):>5} {len(event['asks']):>5}"
                    )

                elif event["event_type"] == "best_bid_ask":
                    market = self.exchange.get_market_from_condition_id(event["market"])
                    outcome = self.exchange.get_outcome_for_asset(event["asset_id"])
                    print(
                        f"BEST_BID_ASK     {market.slug[:50]:<50} {outcome[:30]:<30} {' ':>21} {event['best_bid'][:6]:>6} {event['best_ask'][:6]:>6} {event['spread'][:6]:>6}"
                    )

                elif event["event_type"] == "last_trade_price":
                    market = self.exchange.get_market_from_condition_id(event["market"])
                    outcome = self.exchange.get_outcome_for_asset(event["asset_id"])
                    print(
                        f"LAST_TRADE_PRICE {market.slug[:50]:<50} {outcome[:30]:<30} {pc['side']:>4} {pc['price'][:5]:>5} {pc['size']:>10}"
                    )

                elif event["event_type"] == "tick_size_change":
                    market = self.exchange.get_market_from_condition_id(event["market"])
                    outcome = self.exchange.get_outcome_for_asset(event["asset_id"])
                    print(
                        f"TICK_SIZE_CHANGE {market.slug[:50]:<50} {outcome[:30]:<30} {event['old_tick_size']:>5} > {event['new_tick_size']:>5}"
                    )

                else:
                    print(dumps(event, indent=2))
