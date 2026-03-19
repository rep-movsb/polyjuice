from collections import defaultdict
from dataclasses import asdict, dataclass
from hashlib import md5
from json import loads
from logging import info
from multiprocessing import Pool
from pyarrow import Table

from polyjuice.configurations.duckdb import DuckDBConfiguration
from polyjuice.configurations.index import IndexConfiguration
from polyjuice.dataset import dataset_connection, export_dataset


def conf() -> IndexConfiguration:
    return IndexConfiguration.get()


@dataclass
class MarketInfo:
    id: int | None = None
    conditionId: str = None
    slug: str | None = None
    first_seen: float | None = None
    last_seen: float | None = None
    stream_groups: set[str] | None = None


def stream_events(dataset: str, stream_group: str):
    with dataset_connection(
        dataset, md5(stream_group.encode()).hexdigest()
    ) as connection:
        cursor = connection.execute(
            "SELECT value FROM dataset WHERE stream_group=$stream_group",
            parameters={"stream_group": stream_group},
        )
        rows = cursor.fetchmany(100000)
        while rows:
            for row in rows:
                yield loads(row[0])
            rows = cursor.fetchmany(100000)


def build_partial_index(
    arguments: tuple[str, str],
) -> tuple[dict[str, MarketInfo], int]:
    streams_dataset, stream_group = arguments
    index = defaultdict(MarketInfo)
    total = 0
    for event in stream_events(streams_dataset, stream_group):
        if "event_type" in event and "market" in event:
            market_info: MarketInfo = index[event["market"]]
            if event["event_type"] == "new_market":
                market_info.id = event["id"]
                market_info.conditionId = event["market"]
                market_info.slug = event["slug"]
            if (
                market_info.first_seen is None
                or event["local_timestamp"] < market_info.first_seen
            ):
                market_info.first_seen = event["local_timestamp"]
            if (
                market_info.last_seen is None
                or event["local_timestamp"] > market_info.last_seen
            ):
                market_info.last_seen = event["local_timestamp"]
            market_info.stream_groups = set([stream_group])
            total += 1
    return (index, total, stream_group)


def build_index(streams_dataset: str, index_dataset: str):
    with dataset_connection(streams_dataset) as connection:
        stream_groups = [
            v[0]
            for v in connection.execute(
                "SELECT DISTINCT stream_group FROM dataset"
            ).fetchall()
        ]
    info(f"Found {len(stream_groups)} stream groups")
    index: dict[str, MarketInfo] = dict()
    with Pool(conf().max_workers, maxtasksperchild=conf().max_tasks_per_worker) as pool:
        total = 0
        arguments = [(streams_dataset, stream_group) for stream_group in stream_groups]
        for partial_index, partial_total, stream_group in pool.imap_unordered(
            build_partial_index, arguments
        ):
            info(f"Stream group {stream_group} events {partial_total}")
            for id, market_info in partial_index.items():
                if id in index:
                    market_info.first_seen = min(
                        market_info.first_seen, index[id].first_seen
                    )
                    market_info.last_seen = max(
                        market_info.last_seen, index[id].last_seen
                    )
                    market_info.stream_groups |= index[id].stream_groups
                index[id] = market_info
            total += partial_total
    info(f"Total events {total}, markets {len(index)}")
    export_dataset(
        index_dataset,
        Table.from_pylist(
            [
                asdict(market_info) | {"stream_groups": list(market_info.stream_groups)}
                for market_info in index.values()
            ]
        ),
    )


if __name__ == "__main__":

    from click import Path, argument, command, option
    from logging import basicConfig, INFO

    basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    @command()
    @argument("streams_dataset", type=Path(dir_okay=True))
    @argument("index_dataset", nargs=-1, type=Path(dir_okay=True))
    @option("--duckdb-configuration-file", type=Path(readable=True))
    @option("--configuration-file", type=Path(readable=True))
    def main(
        streams_dataset: str,
        index_dataset: str,
        duckdb_configuration_file: str | None,
        configuration_file: str | None,
    ):
        with DuckDBConfiguration.load(duckdb_configuration_file):
            with IndexConfiguration.load(configuration_file):
                build_index(streams_dataset, index_dataset)

    main()
