from contextlib import asynccontextmanager
from contextvars import ContextVar
from json import JSONDecodeError, dumps, loads
from logging import info, warning
from websockets import connect
from websockets.asyncio.client import ClientConnection
from typing import Any

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


def print_format_event(
    name: str,
    market: Market,
    outcome: str,
    side: str = "",
    price: str = "",
    size: str = "",
    best_bid: str = "",
    best_ask: str = "",
    spread: str = "",
    old_tick_size: str = "",
    new_tick_size: str = "",
):
    print(
        f"{name:<16} {market.slug[:50]:<50} {outcome[:15]:<15} "
        + f"{side:>4} {price[:5]:>5} {size[:10]:>10} "
        + f"{best_bid[:6]:>6} {best_ask[:6]:>6} {spread[:6]:>6} "
        + f"{old_tick_size:>5} {new_tick_size:>5}"
    )


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
                warning(f"Received unexpected message: {message}")
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

                else:
                    self.print_event(event)

    def print_event(self, event: dict[str, Any]):

        if event["event_type"] == "price_change":
            market = self.exchange.get_market_from_condition_id(event["market"])
            for pc in event["price_changes"]:
                outcome = self.exchange.get_outcome_for_asset(pc["asset_id"])
                print_format_event(
                    "PRICE_CHANGE",
                    market,
                    outcome,
                    side=pc["side"],
                    price=pc["price"],
                    size=pc["size"],
                    best_bid=pc["best_bid"],
                    best_ask=pc["best_ask"],
                )

        elif event["event_type"] in [
            "book",
            "best_bid_ask",
            "last_trade_price",
            "tick_size_change",
        ]:
            market = self.exchange.get_market_from_condition_id(event["market"])
            outcome = self.exchange.get_outcome_for_asset(event["asset_id"])

            if event["event_type"] == "book":
                print_format_event("BOOK", market, outcome)

            elif event["event_type"] == "best_bid_ask":
                print_format_event(
                    "BEST_BID_ASK",
                    market,
                    outcome,
                    best_bid=event["best_bid"],
                    best_ask=event["best_ask"],
                    spread=event["spread"],
                )

            elif event["event_type"] == "last_trade_price":
                print_format_event(
                    "LAST_TRADE_PRICE",
                    market,
                    outcome,
                    side=event["side"],
                    price=event["price"],
                    size=event["size"],
                )

            elif event["event_type"] == "tick_size_change":
                print_format_event(
                    "TICK_SIZE_CHANGE",
                    market,
                    outcome,
                    old_tick_size=event["old_tick_size"],
                    new_tick_size=event["new_tick_size"],
                )

        else:
            warning("Unexpected message:")
            warning(dumps(event, indent=2))
