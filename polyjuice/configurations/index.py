from dataclasses import dataclass

from polyjuice.configurations.base import Configuration


@dataclass
class IndexConfiguration(Configuration("POLYJUICE_INDEX_CONFIGURATION_FILE")):
    max_workers: int = 8
    max_tasks_per_worker: int = 8
