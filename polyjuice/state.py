from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from polars import DataType
from sortedcontainers import SortedList
from typing import Any, AsyncIterator, Callable, Generator

from polyjuice.data.dataset import DatasetOrderedStreamReader


@dataclass
class AggregationStatistics:
    changes: int = 0
    total_delay: int = 0
    mean_delay: float = 0


@dataclass
class StateChange:
    timestamp: float
    execute: Callable[[], None]


class State(metaclass=ABCMeta):

    @abstractmethod
    def partitioning_fields(self) -> list[str]: ...

    @abstractmethod
    def schema(self) -> dict[str, DataType]: ...

    @abstractmethod
    async def process_stream(
        self, reader: DatasetOrderedStreamReader
    ) -> Generator[float | StateChange, None, None]: ...

    @abstractmethod
    def export(
        self, timestamp: float, stats: AggregationStatistics
    ) -> list[dict[str, Any]]: ...


END_OF_STREAM = dict()


async def get_state_changes_until(
    timestamp: float,
    stream: AsyncIterator[StateChange],
    last: StateChange | None,
    change_buffer: SortedList,
):
    if last is not None:
        if last.timestamp > timestamp:
            return last
        change_buffer.add(last)
    async for change in stream:
        if change.timestamp > timestamp:
            return change
        change_buffer.add(change)
    return END_OF_STREAM


class OutOfSync(Exception):
    def raise_exception(output_timestamp: float, change: StateChange):
        raise OutOfSync(output_timestamp, change)


async def compute_state(
    reference_clock: AsyncIterator[int],
    *streams: AsyncIterator[StateChange],
    buffer_length=1,
    on_out_of_sync=OutOfSync.raise_exception,
):
    future_changes: list[StateChange | None] = [None] * len(streams)
    change_buffer = SortedList(key=lambda c: c.timestamp)
    buffer_timestamps = SortedList()
    output_timestamp = float("-inf")
    for _ in range(buffer_length):
        buffer_timestamps.add(await anext(reference_clock))
    while True:
        if (
            all(change is END_OF_STREAM for change in future_changes)
            and len(change_buffer) == 0
        ):
            break
        end_timestamp = await anext(reference_clock)
        for i, stream in enumerate(streams):
            if future_changes[i] is END_OF_STREAM:
                continue
            future_changes[i] = await get_state_changes_until(
                end_timestamp, stream, future_changes[i], change_buffer
            )
        buffer_timestamps.add(end_timestamp)
        start_timestamp = buffer_timestamps.pop(0)
        stats = AggregationStatistics()
        while len(change_buffer) > 0:
            change: StateChange = change_buffer.pop(0)
            if change.timestamp <= output_timestamp:
                if not on_out_of_sync(output_timestamp, change):
                    break
            elif change.timestamp > start_timestamp:
                change_buffer.add(change)
                break
            change.execute()
            stats.changes += 1
            stats.total_delay += start_timestamp - change.timestamp
        stats.mean_delay = stats.total_delay / stats.changes if stats.changes > 0 else 0
        output_timestamp = start_timestamp
        yield output_timestamp, stats
