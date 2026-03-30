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


@dataclass
class TaskGroup:
    id: int
    tasks: List[Task] = field(default_factory=list)
