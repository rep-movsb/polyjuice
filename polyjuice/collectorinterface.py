from abc import ABCMeta, abstractmethod
from typing import Any, Iterable

from polyjuice.models import Market


class CollectorInterface(metaclass=ABCMeta):

    @abstractmethod
    async def subscribe_to_market(self, Market): ...

    @abstractmethod
    def get_market_condition_ids(self) -> list[str]: ...

    @abstractmethod
    def get_market_from_condition_id(self, condition_id: str) -> Market | None: ...

    @abstractmethod
    def get_markets(self) -> Iterable[Market]: ...

    @abstractmethod
    def get_outcome_for_asset(self, str) -> str: ...

    @abstractmethod
    def log_event(self, event: dict[str, Any]): ...
