from dataclasses import dataclass

from polyjuice.configurations.base import Configuration


@dataclass
class PartitioningConfiguration(
    Configuration("POLYJUICE_PARTITIONING_CONFIGURATION_FILE")
):
    time_partition_seconds: int = 3600
    rows_per_export: int = 2000000
    rows_per_batch: int = 50000
    max_workers: int = 8
    max_tasks_per_worker: int = 8
    write_queue_size: int = 5
    writer_processes: int = 4
