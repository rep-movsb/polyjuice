from contextlib import ExitStack
from dataclasses import dataclass
from functools import partial
from hashlib import md5
from json import loads
from logging import info
from lzma import open as lzma_open
from multiprocessing import Pool
from typing import Any, Iterable

from polyjuice.configurations.duckdb import DuckDBConfiguration
from polyjuice.configurations.partitioning import PartitioningConfiguration
from polyjuice.dataset import dataset_connection, export_dataset
from polyjuice.index import Index, load_index, merge_indexes, process_event, save_index
from polyjuice.streamgroup import stream_groups_from_event
from polyjuice.utils import batched, list_batched
from polyjuice.writerprocess import WriterProcessHandler, pool_write, writer_process


def conf() -> PartitioningConfiguration:
    return PartitioningConfiguration.get()


@dataclass
class MarketInfo:
    id: int | None = None
    conditionId: str = None
    slug: str | None = None
    first_seen: float | None = None
    last_seen: float | None = None
    stream_groups: set[str] | None = None


def accumulate_export_dataset(output_dataset: str, stream: Iterable[Any]):
    for batch in list_batched(stream, conf().rows_per_export):
        if len(batch) > 0:
            export_dataset(output_dataset, batch, ["stream_group", "time_partition"])


def stream_index_file_data(input_filename: str, index: Index):
    with lzma_open(input_filename, "rb") as input:
        for line in input:
            event = loads(line)
            for stream_group in stream_groups_from_event(event):
                process_event(index, input_filename, event, stream_group)
                yield {
                    "local_timestamp": event["local_timestamp"],
                    "time_partition": int(
                        event["local_timestamp"] / conf().time_partition_seconds
                    )
                    * conf().time_partition_seconds,
                    "filename": input_filename,
                    "stream_group": stream_group,
                    "value": line,
                }


def extract_file(writers: list[WriterProcessHandler], input_filename: str):
    total = 0
    index = Index()
    for chunk in batched(
        stream_index_file_data(input_filename, index), conf().rows_per_batch
    ):
        pool_write(writers, chunk)
        total += len(chunk)
    return (input_filename, index, total)


def partition_streams(
    input_filenames: list[str], index_filename: str, output_directory: str, append: bool
):

    index = load_index(index_filename) if append else Index()

    process_filenames = set(input_filenames) - index.files

    with ExitStack() as stack:
        writers: list[WriterProcessHandler] = []
        for writer_index in range(conf().writer_processes):
            subdataset_directory = f"{output_directory}/{writer_index}"
            writers.append(
                stack.enter_context(
                    writer_process(
                        partial(accumulate_export_dataset, subdataset_directory),
                        conf().write_queue_size,
                    )
                )
            )
        with Pool(
            conf().max_workers, maxtasksperchild=conf().max_tasks_per_worker
        ) as pool:
            total = 0
            for i, (filename, partial_index, partial_total) in enumerate(
                pool.imap_unordered(partial(extract_file, writers), process_filenames)
            ):
                merge_indexes(index, partial_index)
                total += partial_total
                info(f"{i / len(process_filenames):.2f} {partial_total:<8} {filename}")
            # We need to ensure the queues are flushed before the pool is
            # disposed of; an early termination of the feeder process may cause
            # the receiver side to hang otherwise
            for writer in writers:
                writer.join()
        info(f"Total {total} processed events")

    save_index(index_filename, index)


def stream_partitioned_dataset_events(
    dataset: str, stream_group: str, exclude_files: list[str]
):
    with dataset_connection(
        dataset, md5(stream_group.encode()).hexdigest()
    ) as connection:
        cursor = connection.execute(
            """
            SELECT filename, value
            FROM dataset
            WHERE stream_group=$stream_group
            AND filename NOT IN (SELECT * FROM unnest($filenames))
            """,
            parameters={"stream_group": stream_group, "filenames": exclude_files},
        )
        rows = cursor.fetchmany(100000)
        while rows:
            for row in rows:
                yield (row[0], loads(row[1]))
            rows = cursor.fetchmany(100000)


def build_partial_index_from_existing_dataset(
    arguments: tuple[str, str],
) -> tuple[dict[str, MarketInfo], int]:
    streams_dataset, stream_group, exclude_files = arguments
    index = Index()
    total = 0
    for file, event in stream_partitioned_dataset_events(
        streams_dataset, stream_group, exclude_files
    ):
        total += process_event(index, file, event, stream_group)
    return (index, total, stream_group)


def build_index_from_existing_dataset(
    streams_dataset: str, index_filename: str, append: bool
):

    index = load_index(index_filename) if append else Index()

    with dataset_connection(streams_dataset) as connection:
        stream_groups = [
            v[0]
            for v in connection.execute(
                """
                SELECT DISTINCT stream_group
                FROM dataset
                WHERE filename NOT IN (SELECT * FROM unnest($filenames))
                """,
                parameters={"filenames": list(index.files)},
            ).fetchall()
        ]
    info(f"Found {len(stream_groups)} stream groups")

    with Pool(conf().max_workers, maxtasksperchild=conf().max_tasks_per_worker) as pool:
        total = 0
        arguments = [
            (streams_dataset, stream_group, list(index.files))
            for stream_group in stream_groups
        ]
        for partial_index, partial_total, stream_group in pool.imap_unordered(
            build_partial_index_from_existing_dataset, arguments
        ):
            info(f"Stream group {stream_group} events {partial_total}")
            merge_indexes(index, partial_index)
            total += partial_total
    info(f"Total events {total}, markets {len(index.markets)}")

    save_index(index_filename, index)


if __name__ == "__main__":

    from click import Path, argument, group, option
    from logging import basicConfig, INFO

    basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    @group()
    def cli(): ...

    @cli.command()
    @argument("index_file", type=Path())
    @argument("output_dataset", type=Path(dir_okay=True))
    @argument("input_files", nargs=-1, type=Path(exists=True, readable=True))
    @option("--append", is_flag=True)
    @option("--configuration-file", type=Path(readable=True))
    def partition(
        index_file: str,
        output_dataset: str,
        input_files: list[str],
        append: bool,
        configuration_file: str | None,
    ):
        with PartitioningConfiguration.load(configuration_file):
            partition_streams(input_files, index_file, output_dataset, append)

    @cli.command()
    @argument("index_file", type=Path())
    @argument("streams_dataset", type=Path(dir_okay=True))
    @option("--append", is_flag=True)
    @option("--duckdb-configuration-file", type=Path(readable=True))
    @option("--configuration-file", type=Path(readable=True))
    def build_index(
        index_file: str,
        streams_dataset: str,
        append: bool | None,
        duckdb_configuration_file: str | None,
        configuration_file: str | None,
    ):
        with DuckDBConfiguration.load(duckdb_configuration_file):
            with PartitioningConfiguration.load(configuration_file):
                build_index_from_existing_dataset(streams_dataset, index_file, append)

    cli(standalone_mode=False)
