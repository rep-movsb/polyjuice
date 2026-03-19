from typing import Any


def stream_groups_from_event(event: dict[str, Any]):
    if "market" in event:
        return [f"market-{event['market'][2]}"]
    else:
        return ["default"]
