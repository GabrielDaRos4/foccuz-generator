import pandas as pd
import pytest

from src.context.shared.domain.strategies import (
    ConcatMergeStrategy,
    DataMergeStrategyFactory,
    JoinMergeStrategy,
)


class TestJoinMergeStrategy:

    def test_simple_left_join(self):
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'monto': [100, 200, 300]
        })
        df2 = pd.DataFrame({
            'id': [1, 2],
            'nombre': ['A', 'B']
        })

        strategy = JoinMergeStrategy()
        result = strategy.merge(
            {'ventas': df1, 'empleados': df2},
            {
                'primary_source': 'ventas',
                'joins': [
                    {'source': 'empleados', 'on': ['id'], 'how': 'left'}
                ]
            }
        )

        assert len(result) == 3
        assert 'nombre' in result.columns
        assert pd.isna(result.loc[2, 'nombre'])

    def test_inner_join(self):
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'monto': [100, 200, 300]
        })
        df2 = pd.DataFrame({
            'id': [2, 3],
            'nombre': ['B', 'C']
        })

        strategy = JoinMergeStrategy()
        result = strategy.merge(
            {'ventas': df1, 'empleados': df2},
            {
                'primary_source': 'ventas',
                'joins': [
                    {'source': 'empleados', 'on': ['id'], 'how': 'inner'}
                ]
            }
        )

        assert len(result) == 2
        assert list(result['id']) == [2, 3]

    def test_multiple_joins(self):
        df1 = pd.DataFrame({'id': [1, 2], 'monto': [100, 200]})
        df2 = pd.DataFrame({'id': [1, 2], 'nombre': ['A', 'B']})
        df3 = pd.DataFrame({'id': [1, 2], 'depto': ['Ventas', 'Marketing']})

        strategy = JoinMergeStrategy()
        result = strategy.merge(
            {'ventas': df1, 'empleados': df2, 'departamentos': df3},
            {
                'primary_source': 'ventas',
                'joins': [
                    {'source': 'empleados', 'on': ['id'], 'how': 'left'},
                    {'source': 'departamentos', 'on': ['id'], 'how': 'left'}
                ]
            }
        )

        assert len(result) == 2
        assert all(col in result.columns for col in ['monto', 'nombre', 'depto'])

    def test_missing_primary_source_raises_error(self):
        strategy = JoinMergeStrategy()

        with pytest.raises(ValueError, match="requires 'primary_source'"):
            strategy.merge({}, {})

    def test_missing_join_columns_raises_error(self):
        df1 = pd.DataFrame({'id': [1], 'monto': [100]})
        df2 = pd.DataFrame({'id': [1], 'nombre': ['A']})

        strategy = JoinMergeStrategy()

        with pytest.raises(ValueError, match="must specify 'on' columns"):
            strategy.merge(
                {'ventas': df1, 'empleados': df2},
                {
                    'primary_source': 'ventas',
                    'joins': [{'source': 'empleados', 'how': 'left'}]
                }
            )

    def test_missing_primary_source_key_raises(self):
        strategy = JoinMergeStrategy()
        with pytest.raises(KeyError, match="not found in dataframes"):
            strategy.merge(
                {'other': pd.DataFrame()},
                {'primary_source': 'ventas', 'joins': []}
            )

    def test_missing_join_source_raises(self):
        df1 = pd.DataFrame({'id': [1]})
        strategy = JoinMergeStrategy()
        with pytest.raises(ValueError, match="must include 'source'"):
            strategy.merge(
                {'ventas': df1},
                {'primary_source': 'ventas', 'joins': [{'on': ['id']}]}
            )

    def test_missing_join_source_key_raises(self):
        df1 = pd.DataFrame({'id': [1]})
        strategy = JoinMergeStrategy()
        with pytest.raises(KeyError, match="not found in dataframes"):
            strategy.merge(
                {'ventas': df1},
                {'primary_source': 'ventas', 'joins': [{'source': 'missing', 'on': ['id']}]}
            )

    def test_missing_left_column_raises(self):
        df1 = pd.DataFrame({'id': [1]})
        df2 = pd.DataFrame({'id': [1], 'code': ['A']})
        strategy = JoinMergeStrategy()
        with pytest.raises(ValueError, match="not found in left dataframe"):
            strategy.merge(
                {'ventas': df1, 'empleados': df2},
                {'primary_source': 'ventas', 'joins': [
                    {'source': 'empleados', 'on': ['code'], 'how': 'left'}
                ]}
            )

    def test_missing_right_column_raises(self):
        df1 = pd.DataFrame({'id': [1], 'code': ['A']})
        df2 = pd.DataFrame({'id': [1]})
        strategy = JoinMergeStrategy()
        with pytest.raises(ValueError, match="not found in right dataframe"):
            strategy.merge(
                {'ventas': df1, 'empleados': df2},
                {'primary_source': 'ventas', 'joins': [
                    {'source': 'empleados', 'on': ['code'], 'how': 'left'}
                ]}
            )


class TestConcatMergeStrategy:

    def test_concat_rows(self):
        df1 = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
        df2 = pd.DataFrame({'id': [3, 4], 'value': [30, 40]})

        strategy = ConcatMergeStrategy()
        result = strategy.merge(
            {'source1': df1, 'source2': df2},
            {
                'axis': 0,
                'ignore_index': True,
                'source_order': ['source1', 'source2']
            }
        )

        assert len(result) == 4
        assert list(result['value']) == [10, 20, 30, 40]

    def test_concat_columns(self):
        df1 = pd.DataFrame({'a': [1, 2]})
        df2 = pd.DataFrame({'b': [3, 4]})

        strategy = ConcatMergeStrategy()
        result = strategy.merge(
            {'source1': df1, 'source2': df2},
            {
                'axis': 1,
                'ignore_index': False,
                'source_order': ['source1', 'source2']
            }
        )

        assert len(result.columns) == 2
        assert 'a' in result.columns
        assert 'b' in result.columns

    def test_missing_source_raises(self):
        strategy = ConcatMergeStrategy()
        with pytest.raises(KeyError, match="not found in dataframes"):
            strategy.merge(
                {'s1': pd.DataFrame()},
                {'source_order': ['s1', 'missing']}
            )

    def test_respects_source_order(self):
        df1 = pd.DataFrame({'value': [1]})
        df2 = pd.DataFrame({'value': [2]})
        df3 = pd.DataFrame({'value': [3]})

        strategy = ConcatMergeStrategy()
        result = strategy.merge(
            {'s1': df1, 's2': df2, 's3': df3},
            {
                'axis': 0,
                'source_order': ['s3', 's1', 's2']
            }
        )

        assert list(result['value']) == [3, 1, 2]


class TestDataMergeStrategyFactory:

    def test_create_join_strategy(self):
        strategy = DataMergeStrategyFactory.create('join')
        assert isinstance(strategy, JoinMergeStrategy)

    def test_create_concat_strategy(self):
        strategy = DataMergeStrategyFactory.create('concat')
        assert isinstance(strategy, ConcatMergeStrategy)

    def test_unsupported_strategy_raises_error(self):
        with pytest.raises(ValueError, match="not supported"):
            DataMergeStrategyFactory.create('invalid')

    def test_get_supported_types(self):
        types = DataMergeStrategyFactory.get_supported_types()
        assert 'join' in types
        assert 'concat' in types
        assert 'custom' in types

    def test_create_custom_strategy(self):
        def my_merge(dfs, config):
            return list(dfs.values())[0]

        strategy = DataMergeStrategyFactory.create('custom', merge_function=my_merge)
        from src.context.shared.domain.strategies.custom_merge_strategy import CustomMergeStrategy
        assert isinstance(strategy, CustomMergeStrategy)

    def test_create_custom_without_function_raises(self):
        with pytest.raises(ValueError, match="requires 'merge_function'"):
            DataMergeStrategyFactory.create('custom')

    def test_register_custom_strategy(self):
        from src.context.shared.domain.strategies import DataMergeStrategy
        class MyStrategy(DataMergeStrategy):
            def merge(self, dataframes, config):
                return pd.DataFrame()

        DataMergeStrategyFactory.register_strategy('my_custom', MyStrategy)
        assert 'my_custom' in DataMergeStrategyFactory.get_supported_types()

    def test_register_invalid_strategy_raises(self):
        with pytest.raises(ValueError, match="must extend DataMergeStrategy"):
            DataMergeStrategyFactory.register_strategy('invalid', str)


class TestCustomMergeStrategy:

    def test_should_call_merge_function(self):
        from src.context.shared.domain.strategies.custom_merge_strategy import CustomMergeStrategy

        def my_merge(dfs, config):
            return pd.DataFrame({"result": [1, 2, 3]})

        strategy = CustomMergeStrategy(my_merge)
        result = strategy.merge({"src": pd.DataFrame()}, {})

        assert len(result) == 3

    def test_should_raise_on_non_callable(self):
        from src.context.shared.domain.strategies.custom_merge_strategy import CustomMergeStrategy

        with pytest.raises(ValueError, match="must be callable"):
            CustomMergeStrategy("not_a_function")

    def test_should_raise_when_function_returns_non_dataframe(self):
        from src.context.shared.domain.strategies.custom_merge_strategy import CustomMergeStrategy

        def bad_merge(dfs, config):
            return {"not": "a dataframe"}

        strategy = CustomMergeStrategy(bad_merge)
        with pytest.raises(ValueError, match="must return a pandas DataFrame"):
            strategy.merge({"src": pd.DataFrame()}, {})

