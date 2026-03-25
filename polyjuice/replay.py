from asyncio import run
from contextlib import ExitStack
from functools import partial
from logging import info, warning
from multiprocessing import Pool
from typing import Any, Iterable, Type

from polyjuice.configurations.replay import ReplayConfiguration
from polyjuice.data.dataset import (
    DatasetOrderedStreamReader,
    DatasetStream,
    export_dataset,
)
from polyjuice.state import State, StateChange, compute_state
from polyjuice.writerprocess import WriterProcessHandler, writer_process, pool_write


def conf() -> ReplayConfiguration:
    return ReplayConfiguration.get()


class StateClassConfiguration:
    Class: Type[State]
    output_dataset: str
    args: list[Any]
    kwargs: dict[str, Any]

    def __init__(self, Class: str, output_dataset: str, *args, **kwargs):
        self.Class = Class
        self.output_dataset = output_dataset
        self.args = args
        self.kwargs = kwargs


async def reference_clock(start: int, delta: int):
    t = start
    while True:
        yield t
        t += delta


async def stream_global_state(
    state: State, reader: DatasetOrderedStreamReader, dt: int
):
    stream = state.process_stream(reader)
    first_timestamp = await anext(stream)

    def on_out_of_sync(output_timestamp: int, change: StateChange):
        warning(f"Out of sync {change} {output_timestamp - change.timestamp}")
        return True

    async for timestamp, stats in compute_state(
        reference_clock(first_timestamp, dt),
        stream,
        buffer_length=1,
        on_out_of_sync=on_out_of_sync,
    ):
        yield (timestamp, stats)


def accumulate_write_parquet_files(
    dataset_root: str,
    writer_index: int,
    stream: Iterable[Any],
):
    for batch, dataset, partitioning_fields, schema in stream:
        export_dataset(
            f"{dataset_root}/{dataset}/{writer_index}",
            batch,
            partitioning_fields=partitioning_fields,
            schema=schema,
        )


def get_state_configuration_from_stream_group(
    stream_group: str,
    stream_group_to_state_class_map: dict[str, StateClassConfiguration],
):
    for stream_group_prefix, configuration in stream_group_to_state_class_map.items():
        if stream_group.startswith(stream_group_prefix):
            return configuration


def replay_stream_group_sync(
    writers: list[WriterProcessHandler],
    reader: DatasetOrderedStreamReader,
    stream_group_to_state_class_map: dict[str, StateClassConfiguration],
):
    configuration = get_state_configuration_from_stream_group(
        reader.stream_group, stream_group_to_state_class_map
    )
    if configuration is None:
        warning(
            f"State class not found for stream group {reader.stream_group}, skipping"
        )
        return
    state = configuration.Class(*configuration.args, **configuration.kwargs)

    async def replay_stream_group():
        rows = []
        async for timestamp, stats in stream_global_state(
            state, reader, conf().buffer_length
        ):
            rows.extend(state.export(timestamp, stats))
            if len(rows) >= conf().rows_per_batch:
                pool_write(
                    writers,
                    (
                        rows,
                        configuration.output_dataset,
                        state.partitioning_fields(),
                        state.schema(),
                    ),
                )
                del rows
                rows = []
        if len(rows) > 0:
            pool_write(
                writers,
                (
                    rows,
                    configuration.output_dataset,
                    state.partitioning_fields(),
                    state.schema(),
                ),
            )

    info(f"Starting state recovery of stream group {reader.stream_group}")
    try:
        run(replay_stream_group())
    except Exception as ex:
        print(str(ex))
        raise
    info(f"Finished state recovery of stream group {reader.stream_group}")


def replay_all(
    streams_dataset: str,
    states_dataset: str,
    stream_group_to_state_class_map: dict[str, StateClassConfiguration],
):
    readers = DatasetStream(streams_dataset).ordered_readers(None)
    with ExitStack() as stack:
        writers: dict[Type[State], WriterProcessHandler] = []
        for writer_index in range(conf().writer_processes):
            writers.append(
                stack.enter_context(
                    writer_process(
                        partial(
                            accumulate_write_parquet_files, states_dataset, writer_index
                        ),
                        1,
                    )
                )
            )
        with Pool(
            conf().max_workers, maxtasksperchild=conf().max_tasks_per_worker
        ) as pool:
            total = 0
            arguments = [
                (writers, reader, stream_group_to_state_class_map) for reader in readers
            ]
            pool.starmap(replay_stream_group_sync, arguments)
            # We need to ensure the queues are flushed before the pool is
            # disposed of; an early termination of the feeder process may cause
            # the receiver side to hang otherwise
            for writer in writers:
                writer.join()
        info(f"Total {total}")
