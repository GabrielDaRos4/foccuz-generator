import pandas as pd
import pytest

from src.context.commissions.domain.value_objects import CacheKey
from src.context.commissions.infrastructure.cache import InMemoryDataSourceCache


class TestCacheKey:

    def test_should_generate_same_key_for_same_config(self):
        config = {'endpoint': 'https://api.example.com', 'param': 'value'}

        key1 = CacheKey.from_data_source('api', config)
        key2 = CacheKey.from_data_source('api', config)

        assert key1 == key2

    def test_should_generate_different_key_for_different_config(self):
        config1 = {'endpoint': 'https://api.example.com'}
        config2 = {'endpoint': 'https://api.different.com'}

        key1 = CacheKey.from_data_source('api', config1)
        key2 = CacheKey.from_data_source('api', config2)

        assert key1 != key2

    def test_should_generate_different_key_for_different_source_type(self):
        config = {'path': '/data/file.json'}

        key1 = CacheKey.from_data_source('json', config)
        key2 = CacheKey.from_data_source('csv', config)

        assert key1 != key2

    def test_should_generate_consistent_key_regardless_of_dict_order(self):
        config1 = {'a': 1, 'b': 2, 'c': 3}
        config2 = {'c': 3, 'a': 1, 'b': 2}

        key1 = CacheKey.from_data_source('api', config1)
        key2 = CacheKey.from_data_source('api', config2)

        assert key1 == key2


class TestInMemoryDataSourceCache:

    @pytest.fixture
    def cache(self):
        return InMemoryDataSourceCache()

    @pytest.fixture
    def sample_dataframe(self):
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })

    @pytest.fixture
    def sample_key(self):
        return CacheKey.from_data_source('api', {'endpoint': 'https://api.example.com'})

    def test_should_return_none_for_cache_miss(self, cache, sample_key):
        result = cache.get(sample_key)

        assert result is None

    def test_should_return_data_for_cache_hit(self, cache, sample_key, sample_dataframe):
        cache.set(sample_key, sample_dataframe)

        result = cache.get(sample_key)

        assert result is not None
        assert len(result) == 3
        pd.testing.assert_frame_equal(result, sample_dataframe)

    def test_should_return_copy_not_reference(self, cache, sample_key, sample_dataframe):
        cache.set(sample_key, sample_dataframe)

        result = cache.get(sample_key)
        result['id'] = [99, 99, 99]

        cached = cache.get(sample_key)
        assert cached['id'].tolist() == [1, 2, 3]

    def test_should_track_hits_and_misses(self, cache, sample_key, sample_dataframe):
        cache.get(sample_key)
        cache.get(sample_key)

        cache.set(sample_key, sample_dataframe)

        cache.get(sample_key)
        cache.get(sample_key)
        cache.get(sample_key)

        stats = cache.get_stats()
        assert stats.misses == 2
        assert stats.hits == 3

    def test_should_clear_cache_and_reset_stats(self, cache, sample_key, sample_dataframe):
        cache.set(sample_key, sample_dataframe)
        cache.get(sample_key)

        cache.clear()

        assert cache.get(sample_key) is None
        stats = cache.get_stats()
        assert stats.hits == 0
        assert stats.misses == 1
        assert stats.entries == 0

    def test_should_contain_key_after_set(self, cache, sample_key, sample_dataframe):
        assert not cache.contains(sample_key)

        cache.set(sample_key, sample_dataframe)

        assert cache.contains(sample_key)

    def test_should_calculate_hit_rate(self, cache, sample_key, sample_dataframe):
        cache.get(sample_key)
        cache.set(sample_key, sample_dataframe)
        cache.get(sample_key)
        cache.get(sample_key)
        cache.get(sample_key)

        stats = cache.get_stats()

        assert stats.hit_rate == 0.75

    def test_should_handle_empty_dataframe(self, cache, sample_key):
        empty_df = pd.DataFrame()
        cache.set(sample_key, empty_df)

        result = cache.get(sample_key)

        assert result is not None
        assert result.empty

    def test_should_count_total_rows(self, cache, sample_dataframe):
        key1 = CacheKey.from_data_source('api', {'id': '1'})
        key2 = CacheKey.from_data_source('api', {'id': '2'})

        cache.set(key1, sample_dataframe)
        cache.set(key2, sample_dataframe)

        stats = cache.get_stats()

        assert stats.entries == 2
        assert stats.total_rows == 6
