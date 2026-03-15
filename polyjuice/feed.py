from abc import ABCMeta, abstractmethod

from polyjuice.collectorinterface import CollectorInterface


class Feed(metaclass=ABCMeta):

    @abstractmethod
    async def initialize(self, exchange: CollectorInterface): ...

    @abstractmethod
    async def fetch_loop(self): ...
