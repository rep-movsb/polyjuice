from dataclasses import dataclass

from polyjuice.configurations.base import Configuration


@dataclass
class DuckDBConfiguration(Configuration("POLYJUICE_DUCKDB_CONFIGURATION_FILE")):
    memory_limit: str | None = None
    threads: int | None = None
    temp_directory: str | None = None
