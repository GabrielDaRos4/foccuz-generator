from collections.abc import Callable
from typing import TypeVar

from .provider import Provider

T = TypeVar("T")


class SingletonProvider(Provider[T]):
    def __init__(self, factory: Callable[[], T]):
        super().__init__(factory)
        self._instance = None

    def get(self) -> T:
        if self._instance is None:
            self._instance = super().get()
        return self._instance
