from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    ClientClassification,
    ClientType,
)


class TestClientClassification:

    def test_new_client_with_bonus(self):
        classification = ClientClassification(is_new=True, gets_bonus=True)

        assert classification.is_new is True
        assert classification.gets_bonus is True
        assert classification.client_type == ClientType.NEW_WITH_BONUS

    def test_new_client_without_bonus(self):
        classification = ClientClassification(is_new=True, gets_bonus=False)

        assert classification.is_new is True
        assert classification.gets_bonus is False
        assert classification.client_type == ClientType.NEW_WITHOUT_BONUS

    def test_existing_client(self):
        classification = ClientClassification(is_new=False, gets_bonus=False)

        assert classification.is_new is False
        assert classification.client_type == ClientType.EXISTING

    def test_existing_client_ignores_bonus_flag(self):
        classification = ClientClassification(is_new=False, gets_bonus=True)

        assert classification.client_type == ClientType.EXISTING
