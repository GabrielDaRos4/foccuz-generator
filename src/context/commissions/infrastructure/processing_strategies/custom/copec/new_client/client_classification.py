from dataclasses import dataclass

from .client_type import ClientType


@dataclass
class ClientClassification:
    is_new: bool
    gets_bonus: bool

    @property
    def client_type(self) -> ClientType:
        if not self.is_new:
            return ClientType.EXISTING
        return ClientType.NEW_WITH_BONUS if self.gets_bonus else ClientType.NEW_WITHOUT_BONUS
