from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from duckdb import connect
from hashlib import md5
from logging import info
from polars import from_dicts
from pyarrow import default_memory_pool
from pyarrow.dataset import ParquetFileFormat, write_dataset
from time import monotonic, time
from typing import Any

from polyjuice.configurations.duckdb import DuckDBConfiguration
from polyjuice.data.stream import Stream, StreamReader
from polyjuice.streamgroup import time_partition_from_timestamp
from polyjuice.utils import batched


def duckdb_conf() -> DuckDBConfiguration:
    return DuckDBConfiguration.get()


@contextmanager
def dataset_connection(dataset_directory: str, temp_directory_id: str | None = None):
    with connect() as connection:
        if duckdb_conf().memory_limit:
            connection.execute(
                "SET memory_limit=$memory_limit",
                parameters={"memory_limit": duckdb_conf().memory_limit},
            )
        if duckdb_conf().threads:
            connection.execute(
                "SET threads=$threads", parameters={"threads": duckdb_conf().threads}
            )
        if temp_directory_id:
            if not duckdb_conf().temp_directory:
                raise Exception(
                    "duckdb temp_directory configuration setting is required and not set"
                )
            hashed_id = md5(temp_directory_id.encode()).hexdigest()
            connection.execute(
                "SET temp_directory=$temp_directory",
                parameters={
                    "temp_directory": f"{duckdb_conf().temp_directory}/{hashed_id}"
                },
            )
        connection.execute(
            f"CREATE TEMP VIEW dataset AS SELECT * FROM read_parquet('{dataset_directory}')"
        )
        yield connection


def export_dataset(
    output_dataset: str,
    rows: list[dict[str, Any]],
    partitioning_fields: list[str] | None = None,
    schema: dict[str, Any] | None = None,
    compression: str = "zstd",
):
    file_options = file_options = ParquetFileFormat().make_write_options(
        compression=compression
    )
    dataset_timestamp_ms = int(time() * 1000)
    start = monotonic()
    table = from_dicts(rows, schema=schema).to_arrow()
    table_created = monotonic()
    write_dataset(
        table,
        base_dir=output_dataset,
        format="parquet",
        file_options=file_options,
        partitioning=partitioning_fields,
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
        basename_template=str(dataset_timestamp_ms) + "-{i}.parquet",
    )
    end = monotonic()
    info(
        f"Exported parquet dataset {len(rows)} rows in {end - start:.2f} seconds (write {end - table_created:.2f}s)"
    )
    del table
    default_memory_pool().release_unused()


@dataclass
class DatasetStreamReader(StreamReader):

    dataset_directory: str
    time_partition: int
    stream_group: str
    start_timestamp: int
    n: int
    total: int

    def read(self):
        with dataset_connection(
            self.dataset_directory,
            temp_directory_id=f"{self.time_partition}:{self.stream_group}",
        ) as connection:
            total_rows = 0
            cursor = connection.execute(
                """
                SELECT value FROM dataset
                WHERE time_partition = $time_partition
                AND stream_group = $stream_group
                AND local_timestamp > $start_timestamp
                """,
                parameters={
                    "time_partition": self.time_partition,
                    "stream_group": self.stream_group,
                    "start_timestamp": (
                        self.start_timestamp if self.start_timestamp is not None else 0
                    ),
                },
            )
            while True:
                start = monotonic()
                rows = cursor.fetchmany(100000)
                if not rows:
                    break
                info(f"Fetched {len(rows)} in {monotonic() - start}")
                for row in rows:
                    yield row[0].decode()
                    total_rows += 1
            info(
                f"{self.n / self.total:.2f} {total_rows:<8} {self.time_partition} {self.stream_group}"
            )


@dataclass
class DatasetOrderedStreamReader(StreamReader):

    dataset_directory: str
    time_partitions: set[int]
    stream_group: str
    start_timestamp: int
    n: int
    total: int

    def read(self):
        with dataset_connection(
            self.dataset_directory, temp_directory_id=self.stream_group
        ) as connection:
            total_rows = 0
            info(
                f"{self.stream_group}: Total {len(self.time_partitions)} time partitions"
            )
            for i, time_partition in enumerate(sorted(self.time_partitions)):
                start = monotonic()
                time_partition_rows = 0
                cursor = connection.execute(
                    """
                    SELECT value
                    FROM dataset
                    WHERE stream_group=$stream_group
                    AND time_partition=$time_partition
                    ORDER BY local_timestamp ASC
                    """,
                    parameters={
                        "stream_group": self.stream_group,
                        "time_partition": time_partition,
                    },
                )
                info(f"Executed query in {monotonic() - start}")
                while True:
                    start = monotonic()
                    rows = cursor.fetchmany(100000)
                    if not rows:
                        break
                    info(f"Fetched {len(rows)} in {monotonic() - start}")
                    for row in rows:
                        yield row[0]
                        total_rows += 1
                        time_partition_rows += 1
                info(
                    f"{self.n / self.total:.2f} {i / len(self.time_partitions):.2f} {time_partition_rows:<8} {self.stream_group} {time_partition}"
                )
            info(f"{self.n / self.total:.2f} {total_rows:<8} {self.stream_group}")


class DatasetStream(Stream):

    def __init__(self, dataset_directory: str):
        self.dataset_directory = dataset_directory

    def readers(self, start_timestamp: int | None):
        all_partitions = self.fetch_all_partitions(start_timestamp)
        return [
            DatasetStreamReader(
                self.dataset_directory,
                time_partition,
                stream_group,
                start_timestamp,
                i,
                len(all_partitions),
            )
            for i, (time_partition, stream_group) in enumerate(all_partitions)
        ]

    def ordered_readers(self, start_timestamp: int | None):
        all_partitions = self.fetch_all_partitions(start_timestamp)
        stream_group_to_time_partitions_map = defaultdict(set)
        for time_partition, stream_group in all_partitions:
            stream_group_to_time_partitions_map[stream_group].add(time_partition)
        return [
            DatasetOrderedStreamReader(
                self.dataset_directory,
                time_partitions,
                stream_group,
                start_timestamp,
                i,
                len(stream_group_to_time_partitions_map),
            )
            for i, (stream_group, time_partitions) in enumerate(
                stream_group_to_time_partitions_map.items()
            )
        ]

    def fetch_all_partitions(self, start_timestamp: int | None):
        with dataset_connection(self.dataset_directory) as connection:
            if start_timestamp is not None:
                start_time_partition = time_partition_from_timestamp(start_timestamp)
                return connection.execute(
                    """
                    SELECT DISTINCT time_partition, stream_group
                    FROM dataset
                    WHERE time_partition >= $start_time_partition
                    """,
                    parameters={"start_time_partition": start_time_partition},
                ).fetchall()
            else:
                return connection.execute(
                    "SELECT DISTINCT time_partition, stream_group FROM dataset"
                ).fetchall()

    def get_last_timestamp(self):
        with dataset_connection(self.dataset_directory) as connection:
            last_time_partition = connection.execute(
                "SELECT MAX(time_partition) FROM dataset"
            ).fetchone()[0]
            info(f"Last time partition is {last_time_partition}")
            last_timestamp = connection.execute(
                "SELECT MAX(local_timestamp) FROM dataset WHERE time_partition=$time_partition",
                parameters={"time_partition": last_time_partition},
            ).fetchone()[0]
            info(f"Last local timestamp is {last_timestamp}")
            return last_timestamp
