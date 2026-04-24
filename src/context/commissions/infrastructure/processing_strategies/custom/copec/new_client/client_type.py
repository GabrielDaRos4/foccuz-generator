from enum import Enum


class ClientType(Enum):
    NEW_WITH_BONUS = "NEW_WITH_BONUS"
    NEW_WITHOUT_BONUS = "NEW_WITHOUT_BONUS"
    EXISTING = "EXISTING"
