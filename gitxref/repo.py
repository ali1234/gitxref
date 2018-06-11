import pathlib
from collections import defaultdict

from gitxref.gitproc import GitProc


class Repo(object):
    def __init__(self, path):
        self._path = pathlib.Path(path)
        self._git_dir = pathlib.Path(GitProc(self._path, ['rev-parse', '--absolute-git-dir']).read().strip().decode('utf8'))

    @property
    def git_dir(self):
        return self._git_dir

    def batch_all(self, types=None):
        if types is None:
            types = [b'commit', b'tree', b'blob', b'tag']

        with GitProc(self._git_dir, ['cat-file', '--buffer', '--batch-all-objects',
                                      '--batch-check=%(objectname) %(objecttype)']) as g:
            for line in g:
                tmp = line.strip().split()
                if tmp[1] in types:
                    yield tmp

    def count_objects(self):
        typecount = defaultdict(int)
        for o in self.batch_all():
            typecount[o[1]] += 1
        return typecount

    def for_each_ref(self, args=[]):
        with GitProc(self._git_dir, ['for-each-ref'] + args) as g:
            yield from g


def count():
    """Entry point for `git-count`."""
    import sys
    typecount = Repo(sys.argv[1]).count_objects()
    print(', '.join('{:s}s: {:d}'.format(k.decode('utf8').capitalize(), v) for k, v in sorted(typecount.items())),
          'Total:', sum(typecount.values()))

if __name__ == '__main__':
    count()