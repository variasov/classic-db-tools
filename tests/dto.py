from dataclasses import dataclass, field
from typing import List


@dataclass
class Status:
    id: int
    title: str


@dataclass
class Task:
    id: int
    name: str
    statuses: List[Status] = field(default_factory=list)
