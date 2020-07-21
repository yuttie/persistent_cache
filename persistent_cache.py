import contextlib
import functools
import hashlib
import io
import logging
import os
import pickle

import zstandard as zstd


@contextlib.contextmanager
def zstd_open_write(path, *args, **kwargs):
    with open(path, 'wb') as f:
        cctx = zstd.ZstdCompressor(*args, **kwargs)
        with cctx.stream_writer(f) as comp:
            yield comp


@contextlib.contextmanager
def zstd_open_read(path, *args, **kwargs):
    with open(path, 'rb') as f:
        dctx = zstd.ZstdDecompressor(*args, **kwargs)
        with dctx.stream_reader(f) as decomp:
            yield io.BufferedReader(decomp)


def _hash_args(*args, **kwargs):
    normalized_args = (args, tuple(sorted(kwargs.items())))
    return hashlib.sha1(pickle.dumps(normalized_args)).hexdigest()


def store_cache(value, func, *args, **kwargs):
    logger = logging.getLogger(f'{__name__}.store_cache.{func.__name__}')

    base_cache_dir = '.persistent_cache'
    cache_dir = os.path.join(base_cache_dir, func.__module__ + '.' + func.__qualname__)

    cache_key = _hash_args(*args, **kwargs)
    cache_file = os.path.join(cache_dir, cache_key + '.pickle.zst')

    os.makedirs(cache_dir, exist_ok=True)
    with zstd_open_write(cache_file, level=19, threads=-1) as f:
        pickle.dump(value, f)
    logger.info('Created a cache "%s"', cache_file)


def cache(func):
    logger = logging.getLogger(f'{__name__}.persistent_cache.{func.__name__}')

    base_cache_dir = '.persistent_cache'
    cache_dir = os.path.join(base_cache_dir, func.__module__ + '.' + func.__qualname__)

    @functools.wraps(func)
    def wrapper_cache(*args, **kwargs):
        cache_key = _hash_args(*args, **kwargs)
        cache_file = os.path.join(cache_dir, cache_key + '.pickle.zst')

        if os.path.isfile(cache_file):
            logger.info('Found a cache "%s"', cache_file)
            with zstd_open_read(cache_file) as f:
                return pickle.load(f)
        else:
            logger.info('No cache found, computing the function...')

            value = func(*args, **kwargs)

            logger.info('Computation has finished')

            os.makedirs(cache_dir, exist_ok=True)
            with zstd_open_write(cache_file, level=19, threads=-1) as f:
                pickle.dump(value, f)
            logger.info('Created a cache "%s"', cache_file)

            return value

    return wrapper_cache
