from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Flat:
    id: int
    uuid: str
    url: str
    room: int
    square: int
    city: str
    lat: float
    lon: float
    description: str
    photo: str
    price: int
    green_percentage: float
    address: str
    title: str
    star: int | None = None
    focus: int | None = None
