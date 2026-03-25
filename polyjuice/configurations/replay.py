from dataclasses import dataclass

from polyjuice.configurations.base import Configuration


@dataclass
class ReplayConfiguration(Configuration("POLYJUICE_REPLAY_CONFIGURATION_FILE")):
    buffer_length: float = 1
    rows_per_batch: int = 50000
    max_workers: int = 8
    max_tasks_per_worker: int = 1
    write_queue_size: int = 1
    writer_processes: int = 4
