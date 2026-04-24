from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from src.context.shared.domain.cqrs.query import Query

TResult = TypeVar('TResult')


class QueryHandler(ABC, Generic[TResult]):

    @abstractmethod
    def handle(self, query: Query) -> TResult:
        pass
