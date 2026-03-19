from contextlib import ExitStack
from functools import partial
from json import loads
from logging import info
from lzma import open as lzma_open
from multiprocessing import Pool
from typing import Any, Iterable

from polyjuice.configurations.partitioning import PartitioningConfiguration
from polyjuice.dataset import export_dataset
from polyjuice.streamgroup import stream_groups_from_event
from polyjuice.utils import batched, list_batched
from polyjuice.writerprocess import WriterProcessHandler, pool_write, writer_process


def conf() -> PartitioningConfiguration:
    return PartitioningConfiguration.get()


def accumulate_export_dataset(output_dataset: str, stream: Iterable[Any]):
    for batch in list_batched(stream, conf().rows_per_export):
        if len(batch) > 0:
            export_dataset(output_dataset, batch, ["stream_group", "time_partition"])


def stream_file_data(input_filename: str):
    with lzma_open(input_filename, "rb") as input:
        for line in input:
            event = loads(line)
            for stream_group in stream_groups_from_event(event):
                yield {
                    "local_timestamp": event["local_timestamp"],
                    "time_partition": int(
                        event["local_timestamp"] / conf().time_partition_seconds
                    )
                    * conf().time_partition_seconds(),
                    "filename": input_filename,
                    "stream_group": stream_group,
                    "value": line,
                }


def extract_file(writers: list[WriterProcessHandler], input_filename: str):
    total = 0
    for chunk in batched(stream_file_data(input_filename), conf().rows_per_batch):
        pool_write(writers, chunk)
        total += len(chunk)
    return (input_filename, total)


def partition_streams(input_filenames: list[str], output_directory: str):
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
            for i, (filename, partial_total) in enumerate(
                pool.imap_unordered(partial(extract_file, writers), input_filenames)
            ):
                total += partial_total
                info(f"{i / len(input_filenames):.2f} {partial_total:<8} {filename}")
            # We need to ensure the queues are flushed before the pool is
            # disposed of; an early termination of the feeder process may cause
            # the receiver side to hang otherwise
            for writer in writers:
                writer.join()
        info(f"Total {total}")


if __name__ == "__main__":

    from click import Path, argument, command, option
    from logging import basicConfig, INFO

    basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    @command()
    @argument("output_dataset", type=Path(dir_okay=True))
    @argument("input_files", nargs=-1, type=Path(exists=True, readable=True))
    @option("--configuration-file", type=Path(readable=True))
    def main(
        output_dataset: str, input_files: list[str], configuration_file: str | None
    ):
        with PartitioningConfiguration.load(configuration_file):
            partition_streams(input_files, output_dataset)

    main()
