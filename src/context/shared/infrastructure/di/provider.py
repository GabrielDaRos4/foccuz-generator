from collections.abc import Callable
from typing import Generic, TypeVar

T = TypeVar("T")

class Provider(Generic[T]):
    def __init__(self, factory: Callable[[], T]):
        self.factory = factory

    def get(self) -> T:
        return self.factory()
