import hashlib
import pathlib
import subprocess

from gitxref.cache import Cache

from gitxref.batch import Batch


class GitCmd(object):
    def __init__(self, path):
        self._path = str(path)

    def __getattr__(self, item):
        return lambda *args: subprocess.check_output(['git', '-C', self._path, item.replace('_', '-'), *args])


class Repo(object):
    def __init__(self, path, **cache_args):
        self._path = pathlib.Path(path)
        self._git = GitCmd(str(path))
        self._git_dir = pathlib.Path(self._git.rev_parse('--absolute-git-dir').strip().decode('utf8'))
        self._cache = Cache(self.git_dir, hashlib.sha1(self.git.for_each_ref()).digest(), **cache_args)

    @property
    def cache(self):
        return self._cache

    @property
    def git_dir(self):
        return self._git_dir

    @property
    def git(self):
        return self._git

    @property
    def objects(self):
        with Batch(self, types=['tr', 'c']) as d:
            yield from d

