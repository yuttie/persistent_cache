import pytest

from persistent_cache import persistent_cache, store_cache

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

def test_store_cache(tmpdir):
    tmpdir.chdir()

    @persistent_cache
    def heavy_computation(x):
        time.sleep(1)
        return x * 2

    # Store a value as a cache
    store_cache(42, heavy_computation, 21)

    # With a cache
    start = time.monotonic()
    assert heavy_computation(21) == 42
    end = time.monotonic()

    # This time it must finish much faster
    assert end - start < 1

    # Without a cache
    start = time.monotonic()
    assert heavy_computation(10) == 20
    end = time.monotonic()

    # Computation must take more than 1 second
    assert end - start >= 1

    # With a cache
    start = time.monotonic()
    assert heavy_computation(10) == 20
    end = time.monotonic()

    # This time it must finish much faster
    assert end - start < 1
