import subprocess


class BatchAll(object):
    """Utility class to list all objects in the repository, optionally filtering by type."""

    def __init__(self, gitdir, types=None):
        self.gitdir = gitdir  # path to repo
        if types is None:
            self.types = [b'commit', b'tree', b'blob', b'tag']
        else:
            self.types = types

    def __enter__(self):
        self.proc = subprocess.Popen(['git', '-C', str(self.gitdir), 'cat-file', '--buffer', '--batch-all-objects',
                                      '--batch-check=%(objectname) %(objecttype)'], stdout=subprocess.PIPE)
        return self

    def __exit__(self, *args):
        self.proc.terminate()
        self.proc.wait(timeout=5)
        self.proc.kill()
        self.proc.wait()

    def __iter__(self):
        return self._all()

    def _all(self):
        for line in self.proc.stdout:
            tmp = line.strip().split()
            if tmp[1] in self.types:
                yield tmp


def count():
    """Entry point for `git-count`."""
    import sys
    from collections import defaultdict
    typecount = defaultdict(int)
    with BatchAll(sys.argv[1]) as b:
        for o in b:
            typecount[o[1]] += 1

    print(', '.join('{:s}s: {:d}'.format(k.decode('utf8').capitalize(), v) for k, v in sorted(typecount.items())),
          'Total:', sum(typecount.values()))


if __name__ == '__main__':
    count()