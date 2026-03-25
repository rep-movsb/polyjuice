from dataclasses import dataclass
from hashlib import md5
from re import match
from typing import Any
from yaml import safe_load

from polyjuice.configurations.partitioning import PartitioningConfiguration
from polyjuice.index import Index


@dataclass
class StreamGroup:
    name: str
    partitions: int = 1


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


@dataclass
class StreamGroupRuleSet:

    groups: dict[str, StreamGroup] | None = None
    rules: dict[str, Any] | None = None
    index: Index | None = None

    def load(rules_file: str, index: Index):
        with open(rules_file) as file:
            groups_and_rules = safe_load(file)
            groups = dict()
            for name, group_definition in groups_and_rules["groups"].items():
                groups[name] = StreamGroup(name, group_definition.get("partitions", 1))
            return StreamGroupRuleSet(groups, groups_and_rules["rules"], index)

    def stream_groups_from_event(self, event: dict[str, Any]):
        if self.rules is None:
            yield from partitions_from_event(event)
            return

        for rule in self.rules:
            if "match" in rule and not match_all(event, self.index, rule["match"]):
                continue

            if "partition_by" in rule:
                attribute = get_attribute(event, rule["partition_by"], self.index)
                partition_number = (
                    int.from_bytes(md5(attribute.encode()).digest())
                    % self.groups[rule["group"]].partitions
                )
                yield f"{rule['group']}-{partition_number}"
            else:
                for i in range(self.groups[rule["group"]].partitions):
                    yield f"{rule['group']}-{i}"

            if not rule.get("continue", False):
                break
