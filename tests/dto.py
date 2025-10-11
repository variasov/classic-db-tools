from dataclasses import dataclass, field
from typing import Optional, List

from classic.components import factory


# @dataclass
# class Status:
#     name: str
#     timestamp: int
#
#
# @dataclass
# class Task:
#     id: int
#     name: int
#     rank: Optional[int]
#     statuses: List[Status] = field(default_factory=list)


@dataclass
class Status:
    id: int
    title: str


@dataclass
class Task:
    id: int
    name: str
    statuses: list['Status'] = factory(list)
