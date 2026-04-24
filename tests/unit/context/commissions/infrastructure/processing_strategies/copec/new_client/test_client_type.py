from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    ClientType,
)


class TestClientType:

    def test_new_with_bonus_value(self):
        assert ClientType.NEW_WITH_BONUS.value == "NEW_WITH_BONUS"

    def test_new_without_bonus_value(self):
        assert ClientType.NEW_WITHOUT_BONUS.value == "NEW_WITHOUT_BONUS"

    def test_existing_value(self):
        assert ClientType.EXISTING.value == "EXISTING"

    def test_all_values_are_unique(self):
        values = [t.value for t in ClientType]
        assert len(values) == len(set(values))
