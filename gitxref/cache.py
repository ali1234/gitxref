import hashlib
import pickle


class Cache(object):
    """
    Implements a dictionary-like interface to pickle that checks for cache validity.

    This is basically a simple version of the shelve module.

    """

    def __init__(self, repo):
        self._repo = repo
        self._hash = hashlib.sha1(self._repo.git.for_each_ref()).digest()

    def _check(self, item):
        check_file = self._repo.git_dir / (item + '.check')
        return check_file.is_file() and self._hash == check_file.read_bytes()

    def __getitem__(self, item):
        if not self._check(item):
            raise KeyError
        cache_file = self._repo.git_dir / (item + '.cache')
        try:
            with cache_file.open('rb') as f:
                return pickle.load(f)
        except Exception:
            raise KeyError

    def __setitem__(self, item, value):
        cache_file = self._repo.git_dir / (item + '.cache')
        with cache_file.open('wb') as f:
            pickle.dump(value, f)
        check_file = self._repo.git_dir / (item + '.check')
        check_file.write_bytes(self._hash)
