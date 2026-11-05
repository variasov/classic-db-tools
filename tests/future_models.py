"""Модель с отложенными аннотациями (PEP 563) для тестов авто-отношений."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FutureStatus:
    title: str


@dataclass
class FutureTask:
    id: int
    statuses: list[FutureStatus] = field(default_factory=list)
    status: FutureStatus | None = None
