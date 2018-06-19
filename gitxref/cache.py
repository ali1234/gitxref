import pathlib
import pickle
import sys


class Cache(object):
    """
    Implements a dictionary-like interface to pickle that checks for cache validity.

    This is basically a simple version of the shelve module.

    """

    def __init__(self, path, hash, force_rebuild=False, skip_cache=False):
        self._path = pathlib.Path(path)
        self._force_rebuild = force_rebuild
        self._skip_cache = skip_cache
        self._hash = hash

    def __contains__(self, item):
        if self._force_rebuild or self._skip_cache:
            return False
        check_file = self._path / (item + '.check')
        return check_file.is_file() and self._hash == check_file.read_bytes()

    def __getitem__(self, item):
        if item not in self:
            raise KeyError
        cache_file = self._path / (item + '.cache')
        try:
            with cache_file.open('rb') as f:
                print('Loading', item, 'from cache...', file=sys.stderr)
                return pickle.load(f)
        except Exception:
            raise KeyError

    def __setitem__(self, item, value):
        if self._skip_cache:
            return
        print('Saving', item, type(value).__name__, 'to cache...', file=sys.stderr)
        cache_file = self._path / (item + '.cache')
        with cache_file.open('wb') as f:
            pickle.dump(value, f)
        check_file = self._path / (item + '.check')
        check_file.write_bytes(self._hash)


if __name__ == '__main__':
    a = [123, 456, 789]
    b = [a, a, a]
    c = Cache('.', b'0')
    c['test'] = b
    assert('test' in c)
    d = c['test']
    assert(d[0] is d[1])
    assert(d[0] is d[2])
