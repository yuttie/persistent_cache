import pytest

from persistent_cache import persistent_cache

import time

def test_persistent_cache(tmpdir):
    tmpdir.chdir()

    @persistent_cache
    def heavy_computation(x):
        time.sleep(1)
        return x * 2

    # Without a cache
    start = time.monotonic()
    assert heavy_computation(21) == 42
    end = time.monotonic()

    # Computation must take more than 1 second
    assert end - start >= 1

    # With a cache
    start = time.monotonic()
    assert heavy_computation(21) == 42
    end = time.monotonic()

    # This time it must finish much faster
    assert end - start < 1
