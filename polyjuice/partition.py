from contextlib import ExitStack
from functools import partial
from json import loads
from logging import info
from multiprocessing import Pool
from traceback import print_exc
from typing import Any, Generator, Iterable
from yaml import safe_load

from polyjuice.configurations.duckdb import DuckDBConfiguration
from polyjuice.configurations.partitioning import PartitioningConfiguration
from polyjuice.dataset import DatasetStream, export_dataset
from polyjuice.index import Index
from polyjuice.raw import RawStream
from polyjuice.stream import Stream, StreamReader
from polyjuice.streamgroup import (
    time_partition_from_timestamp,
    stream_groups_from_event,
)
from polyjuice.utils import batched, list_batched
from polyjuice.writerprocess import WriterProcessHandler, pool_write, writer_process


def conf() -> PartitioningConfiguration:
    return PartitioningConfiguration.get()


def accumulate_export_dataset(output_dataset: str, stream: Iterable[Any]):
    for batch in list_batched(stream, conf().rows_per_export):
        if len(batch) > 0:
            export_dataset(output_dataset, batch, ["stream_group", "time_partition"])


def extract_info(
    lines: Generator[str, None, None],
    index: Index,
    grouping_rules: dict[str, Any] | None = None,
):
    for line in lines:
        event = loads(line)
        for stream_group in stream_groups_from_event(event, index, grouping_rules):
            time_partition = time_partition_from_timestamp(event["local_timestamp"])
            index.process_event(event, stream_group)
            yield {
                "local_timestamp": event["local_timestamp"],
                "time_partition": time_partition,
                "stream_group": stream_group,
                "value": line,
            }


def write_event_batches(
    grouping_rules: dict[str, Any],
    index: Index,
    writers: list[WriterProcessHandler],
    reader: StreamReader,
):
    try:
        total = 0
        for chunk in batched(
            extract_info(reader.read(), index, grouping_rules), conf().rows_per_batch
        ):
            pool_write(writers, chunk)
            total += len(chunk)
        return index, total
    except:
        print_exc()
        raise


def partition_streams(
    stream: Stream,
    index: Index,
    output_dataset: str,
    last_timestamp: int | None = None,
    grouping_rules: dict[str, Any] | None = None,
):
    readers = stream.readers(last_timestamp)
    info(f"{len(readers)} chunks will be processed")

    with ExitStack() as stack:
        writers: list[WriterProcessHandler] = []
        for writer_index in range(conf().writer_processes):
            subdataset_directory = f"{output_dataset}/{writer_index}"
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
            for partial_index, partial_total in pool.imap_unordered(
                partial(write_event_batches, grouping_rules, index, writers), readers
            ):
                index.update(partial_index)
                total += partial_total
            # We need to ensure the queues are flushed before the pool is
            # disposed of; an early termination of the feeder process may cause
            # the receiver side to hang otherwise
            for writer in writers:
                writer.join()
        info(f"Total {total} processed events")

    return total


def build_partial_index_from_existing_dataset(reader: StreamReader):
    index = Index()
    total = 0
    for _ in extract_info(reader.read(), index):
        total += 1
    return (index, total)


def build_index_from_existing_dataset(dataset: str, index_filename: str):

    index = Index()
    readers = DatasetStream(dataset).readers(None)
    info(f"{len(readers)} chunks will be processed")

    with Pool(conf().max_workers, maxtasksperchild=conf().max_tasks_per_worker) as pool:
        total = 0
        for partial_index, partial_total in pool.imap_unordered(
            build_partial_index_from_existing_dataset, readers
        ):
            index.update(partial_index)
            total += partial_total
    info(f"Total events {total}, markets {len(index.markets)}")

    index.save(index_filename)


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
    @option("--duckdb-configuration-file", type=Path(readable=True))
    @option("--configuration-file", type=Path(readable=True))
    def raw(
        index_file: str,
        output_dataset: str,
        input_files: list[str],
        append: bool,
        duckdb_configuration_file: str | None,
        configuration_file: str | None,
    ):
        with ExitStack() as stack:
            if append:
                stack.enter_context(DuckDBConfiguration.load(duckdb_configuration_file))
                index = Index.load(index_file)
                last_timestamp = DatasetStream(output_dataset).get_last_timestamp()
            else:
                index = Index()
                last_timestamp = None
            stack.enter_context(PartitioningConfiguration.load(configuration_file))
            stream = RawStream(input_files)
            if partition_streams(stream, index, output_dataset, last_timestamp) > 0:
                info(f"Saving index file {index_file}")
                index.save(index_file)

    @cli.command()
    @argument("index_file", type=Path())
    @argument("input_dataset", type=Path(exists=True, dir_okay=True))
    @argument("output_dataset", type=Path(dir_okay=True))
    @option("--append", is_flag=True)
    @option("--duckdb-configuration-file", type=Path(readable=True))
    @option("--configuration-file", type=Path(readable=True))
    @option("--grouping-rules-file", type=Path(readable=True))
    def dataset(
        index_file: str,
        input_dataset: str,
        output_dataset: str,
        append: bool,
        duckdb_configuration_file: str | None,
        configuration_file: str | None,
        grouping_rules_file: str | None,
    ):
        with open(grouping_rules_file) as file:
            grouping_rules = safe_load(file)
        index = Index.load(index_file)
        with ExitStack() as stack:
            stack.enter_context(DuckDBConfiguration.load(duckdb_configuration_file))
            stack.enter_context(PartitioningConfiguration.load(configuration_file))
            stream = DatasetStream(input_dataset)
            partition_streams(
                stream,
                index,
                output_dataset,
                DatasetStream(output_dataset).get_last_timestamp() if append else None,
                grouping_rules,
            )

    @cli.command()
    @argument("index_file", type=Path())
    @argument("dataset", type=Path(dir_okay=True))
    @option("--duckdb-configuration-file", type=Path(readable=True))
    @option("--configuration-file", type=Path(readable=True))
    def index(
        index_file: str,
        dataset: str,
        duckdb_configuration_file: str | None,
        configuration_file: str | None,
    ):
        with DuckDBConfiguration.load(duckdb_configuration_file):
            with PartitioningConfiguration.load(configuration_file):
                build_index_from_existing_dataset(dataset, index_file)

    cli(standalone_mode=False)
