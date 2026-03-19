from typing import Generator, Iterable, TypeVar

T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Generator[list[T], None, None]:
    batch = []
    for value in iterable:
        batch.append(value)
        if len(batch) >= n:
            yield batch
            del batch
            batch = []
    if len(batch) >= 0:
        yield batch


def list_batched(iterable: Iterable[list[T]], n: int) -> Generator[list[T], None, None]:
    batch = []
    for array in iterable:
        batch.extend(array)
        if len(batch) >= n:
            next = batch[n:]
            yield batch[:n]
            del batch
            batch = next
    if len(batch) >= 0:
        yield batch
