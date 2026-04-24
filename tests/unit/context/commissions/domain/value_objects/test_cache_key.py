from src.context.commissions.domain.value_objects.cache_key import CacheKey


class TestCacheKey:

    def test_should_create_from_data_source(self):
        key = CacheKey.from_data_source("s3", {"bucket": "test", "key": "file.csv"})

        assert key.key.startswith("s3_")
        assert len(key.key) > 3

    def test_should_be_deterministic(self):
        key1 = CacheKey.from_data_source("s3", {"bucket": "b", "key": "k"})
        key2 = CacheKey.from_data_source("s3", {"bucket": "b", "key": "k"})

        assert key1 == key2

    def test_should_differ_for_different_configs(self):
        key1 = CacheKey.from_data_source("s3", {"bucket": "a"})
        key2 = CacheKey.from_data_source("s3", {"bucket": "b"})

        assert key1 != key2

    def test_str_returns_key(self):
        key = CacheKey(key="test_key")
        assert str(key) == "test_key"

    def test_hash_returns_key_hash(self):
        key = CacheKey(key="test_key")
        assert hash(key) == hash("test_key")
