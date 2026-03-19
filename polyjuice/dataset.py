from contextlib import contextmanager
from duckdb import connect
from logging import info
from polars import from_dicts
from pyarrow import default_memory_pool
from pyarrow.dataset import ParquetFileFormat, write_dataset
from time import monotonic, time
from typing import Any

from polyjuice.configurations.duckdb import DuckDBConfiguration


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
            connection.execute(
                "SET temp_directory=$temp_directory",
                parameters={
                    "temp_directory": f"{duckdb_conf().temp_directory}/{temp_directory_id}"
                },
            )
        connection.execute(
            f"CREATE TEMP VIEW dataset AS SELECT * FROM read_parquet('{dataset_directory}')"
        )
        yield connection


def export_dataset(
    output_dataset: str,
    rows: list[dict[str, Any]],
    partitioning_fields: list[str] | None,
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
