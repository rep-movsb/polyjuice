from dataclasses import dataclass


@dataclass
class Asset:
    id: str
    outcome: str


@dataclass
class Market:
    id: str
    conditionId: str
    slug: str
    assets: list[Asset]

    def from_dict(**values):
        market = Market(**values)
        market.assets = [Asset(**values) for values in market.assets]
        return market
