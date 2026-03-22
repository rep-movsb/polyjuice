from hashlib import md5
from re import match
from typing import Any

from polyjuice.configurations.partitioning import PartitioningConfiguration
from polyjuice.index import Index


def conf() -> PartitioningConfiguration:
    return PartitioningConfiguration.get()


def time_partition_from_timestamp(timestamp: int):
    return (
        int(timestamp / conf().time_partition_seconds) * conf().time_partition_seconds
    )


def partition_from_market_conditionId(id: str):
    return int.from_bytes(md5(id.encode()).digest()) % 8


def partitions_from_event(event: dict[str, Any]):
    if "market" in event:
        partition = partition_from_market_conditionId(event["market"])
        yield f"market-{partition}"
    else:
        yield "default"


def traverse(object: dict[str, Any], path: list[str] = []):
    attribute, *next = path
    if len(next) == 0:
        return object.get(attribute, None)
    if attribute not in object:
        return None
    return traverse(object[attribute], next)


def get_attribute(event: dict[str, Any], attribute_path: str, index: Index):
    root, *path = attribute_path.split(".")
    if root == "market":
        if "market" in event and event["market"] in index.markets:
            return getattr(index.markets[event["market"]], path[0])
        return None
    elif root == "event":
        return traverse(event, path)
    return None


def match_all(event: dict[str, Any], index: Index, match_patterns: dict[str, str]):
    for attribute_path, pattern in match_patterns.items():
        attribute = get_attribute(event, attribute_path, index)
        if attribute is None or not match(pattern, attribute):
            return False
    return True


def stream_groups_from_event(
    event: dict[str, Any], index: Index, rules: dict[str, Any] | None
):
    if rules is None:
        yield from partitions_from_event(event)
        return

    for rule in rules:
        if "match" in rule and not match_all(event, index, rule["match"]):
            continue

        yield rule["group"]

        if not rule.get("continue", False):
            break
