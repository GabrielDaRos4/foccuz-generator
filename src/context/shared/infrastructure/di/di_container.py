from collections.abc import Callable
from typing import TypeVar

from .provider import Provider
from .singleton_provider import SingletonProvider

T = TypeVar("T")


class DIContainer:
    def __init__(self):
        self._providers: dict[type, Provider] = {}

    def register_transient(self, iface: type[T], factory: Callable[[], T]) -> None:
        self._providers[iface] = Provider(factory)

    def register_singleton(self, iface: type[T], factory: Callable[[], T]) -> None:
        self._providers[iface] = SingletonProvider(factory)

    def resolve(self, iface: type[T]) -> T:
        provider = self._providers.get(iface)
        if not provider:
            raise ValueError(f"No provider registered for {iface}")
        return provider.get()
