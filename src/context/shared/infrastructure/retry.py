import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_DELAY = 1
MAX_DELAY = 60
BACKOFF_FACTOR = 2

RATE_LIMIT_INDICATORS = ('429', 'rate limit', 'quota', 'too many requests')


def retry_on_rate_limit(max_retries=MAX_RETRIES, initial_delay=INITIAL_DELAY):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_str = str(e).lower()
                    is_rate_limit = any(
                        indicator in error_str for indicator in RATE_LIMIT_INDICATORS
                    )

                    if not is_rate_limit or attempt == max_retries:
                        raise

                    logger.warning(
                        f"Rate limit hit, attempt {attempt + 1}/{max_retries + 1}. "
                        f"Waiting {delay}s before retry..."
                    )
                    time.sleep(delay)
                    delay = min(delay * BACKOFF_FACTOR, MAX_DELAY)

        return wrapper
    return decorator
