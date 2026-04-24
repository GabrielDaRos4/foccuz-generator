from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from src.context.shared.domain.cqrs.command import Command

TResult = TypeVar('TResult')


class CommandHandler(ABC, Generic[TResult]):

    @abstractmethod
    def handle(self, command: Command) -> TResult:
        pass
