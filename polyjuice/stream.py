from abc import ABCMeta, abstractmethod
from typing import Generator, Iterable


class StreamReader(metaclass=ABCMeta):

    @abstractmethod
    def read() -> Generator[str, None, None]: ...


class Stream(metaclass=ABCMeta):

    @abstractmethod
    def readers(
        self, start_timestamp: int | None
    ) -> Iterable[Generator[str, None, None]]: ...
