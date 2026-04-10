from dataclasses import dataclass

from polyjuice.configurations.base import Configuration


@dataclass
class ReplayConfiguration(Configuration("POLYJUICE_REPLAY_CONFIGURATION_FILE")):
    time_step_seconds: float = 1
    buffer_length_seconds: float = 1
    rows_per_batch: int = 30000
    max_workers: int = 8
    max_tasks_per_worker: int = 1
    write_queue_size: int = 2
    writer_processes: int = 6
