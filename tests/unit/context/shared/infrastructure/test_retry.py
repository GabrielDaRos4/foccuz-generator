import pytest

from src.context.shared.infrastructure.retry import retry_on_rate_limit


class TestRetryOnRateLimit:

    def test_should_return_result_on_success(self):
        @retry_on_rate_limit(max_retries=3, initial_delay=0.01)
        def success():
            return "ok"

        assert success() == "ok"

    def test_should_retry_on_rate_limit_error(self):
        call_count = 0

        @retry_on_rate_limit(max_retries=3, initial_delay=0.01)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("429 Too Many Requests")
            return "ok"

        assert flaky() == "ok"
        assert call_count == 3

    def test_should_retry_on_quota_error(self):
        call_count = 0

        @retry_on_rate_limit(max_retries=3, initial_delay=0.01)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("quota exceeded")
            return "ok"

        assert flaky() == "ok"
        assert call_count == 2

    def test_should_retry_on_rate_limit_keyword(self):
        call_count = 0

        @retry_on_rate_limit(max_retries=3, initial_delay=0.01)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("rate limit hit")
            return "ok"

        assert flaky() == "ok"

    def test_should_retry_on_too_many_requests(self):
        call_count = 0

        @retry_on_rate_limit(max_retries=3, initial_delay=0.01)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("too many requests")
            return "ok"

        assert flaky() == "ok"

    def test_should_raise_immediately_on_non_rate_limit_error(self):
        @retry_on_rate_limit(max_retries=3, initial_delay=0.01)
        def always_fail():
            raise ValueError("bad input")

        with pytest.raises(ValueError, match="bad input"):
            always_fail()

    def test_should_raise_after_max_retries_exhausted(self):
        @retry_on_rate_limit(max_retries=2, initial_delay=0.01)
        def always_rate_limited():
            raise Exception("429 rate limited")

        with pytest.raises(Exception, match="429 rate limited"):
            always_rate_limited()
