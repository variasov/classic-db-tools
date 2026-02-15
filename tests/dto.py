from dataclasses import dataclass, field
from typing import Optional, List

from classic.components import factory


@dataclass
class Status:
    id: int
    title: str


@dataclass
class Task:
    id: int
    name: str
    statuses: list['Status'] = factory(list)
