import pathlib
import subprocess

from gitxref.dump import Dump


class GitCmd(object):
    def __init__(self, path):
        self._path = str(path)

    def __getattr__(self, item):
        return lambda *args: subprocess.check_output(['git', '-C', self._path, item.replace('_', '-'), *args])


class Repo(object):
    def __init__(self, path):
        self._path = pathlib.Path(path)
        self._git = GitCmd(str(path))
        self._git_dir = pathlib.Path(self._git.rev_parse('--absolute-git-dir').strip().decode('utf8'))

    @property
    def git_dir(self):
        return self._git_dir

    @property
    def git(self):
        return self._git

    @property
    def objects(self):
        with Dump(self) as d:
            yield from d.objects()

if __name__ == '__main__':
    count()
