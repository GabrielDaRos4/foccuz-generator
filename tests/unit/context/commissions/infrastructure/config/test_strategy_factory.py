
import pytest

from src.context.commissions.domain.ports import ProcessingStrategy
from src.context.commissions.domain.value_objects import StrategyConfig
from src.context.commissions.infrastructure.config import (
    DynamicStrategyFactory,
)


class MockStrategy(ProcessingStrategy):
    def __init__(self, param1: str = "default"):
        self.param1 = param1

    def calculate_commission(self, data):
        return data


class NotAStrategy:
    pass


class TestDynamicStrategyFactory:

    @pytest.fixture
    def factory(self):
        return DynamicStrategyFactory()

    @pytest.fixture
    def strategy_config(self):
        return StrategyConfig(
            module="tests.unit.context.commissions.infrastructure.config.test_strategy_factory",
            class_name="MockStrategy",
            params={"param1": "test_value"}
        )

    def test_creates_strategy_instance(self, factory, strategy_config):
        strategy = factory.create_strategy(strategy_config)

        assert isinstance(strategy, MockStrategy)
        assert strategy.param1 == "test_value"

    def test_caches_strategy_class(self, factory, strategy_config):
        factory.create_strategy(strategy_config)
        factory.create_strategy(strategy_config)

        cache_key = f"{strategy_config.module}.{strategy_config.class_name}"
        assert cache_key in factory._cache

    def test_clear_cache(self, factory, strategy_config):
        factory.create_strategy(strategy_config)
        assert len(factory._cache) > 0

        factory.clear_cache()
        assert len(factory._cache) == 0

    def test_raises_import_error_for_invalid_module(self, factory):
        config = StrategyConfig(
            module="non.existent.module",
            class_name="SomeClass",
            params={}
        )

        with pytest.raises(ImportError):
            factory.create_strategy(config)

    def test_raises_attribute_error_for_invalid_class(self, factory):
        config = StrategyConfig(
            module="tests.unit.context.commissions.infrastructure.config.test_strategy_factory",
            class_name="NonExistentClass",
            params={}
        )

        with pytest.raises(AttributeError):
            factory.create_strategy(config)

    def test_raises_type_error_for_non_strategy_class(self, factory):
        config = StrategyConfig(
            module="tests.unit.context.commissions.infrastructure.config.test_strategy_factory",
            class_name="NotAStrategy",
            params={}
        )

        with pytest.raises(TypeError) as exc_info:
            factory.create_strategy(config)

        assert "not a subclass of ProcessingStrategy" in str(exc_info.value)

    def test_creates_strategy_with_default_params(self, factory):
        config = StrategyConfig(
            module="tests.unit.context.commissions.infrastructure.config.test_strategy_factory",
            class_name="MockStrategy",
            params={}
        )

        strategy = factory.create_strategy(config)

        assert strategy.param1 == "default"
