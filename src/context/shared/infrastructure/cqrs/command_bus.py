from typing import TypeVar

from src.context.shared.domain.cqrs import Command, CommandHandler

TResult = TypeVar('TResult')


class CommandBus:
    def __init__(self):
        self._handlers: dict[type[Command], CommandHandler[object]] = {}

    def register(
        self, command_type: type[Command], handler: CommandHandler[TResult]
    ) -> None:
        self._handlers[command_type] = handler

    def execute(self, command: Command) -> object:
        handler = self._handlers.get(type(command))
        if not handler:
            raise ValueError(f"No handler registered for {type(command).__name__}")
        return handler.handle(command)
