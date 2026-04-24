from typing import TypeVar

from src.context.shared.domain.cqrs import Query, QueryHandler

TResult = TypeVar('TResult')


class QueryBus:
    def __init__(self):
        self._handlers: dict[type[Query], QueryHandler[object]] = {}

    def register(self, query_type: type[Query], handler: QueryHandler[TResult]) -> None:
        self._handlers[query_type] = handler

    def execute(self, query: Query) -> object:
        handler = self._handlers.get(type(query))
        if not handler:
            raise ValueError(f"No handler registered for {type(query).__name__}")
        return handler.handle(query)
