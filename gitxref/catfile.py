import multiprocessing
import subprocess

from binascii import unhexlify

class CatFileThread(object):

    def __init__(self, repo):
        self._repo = repo

    def initializer(self):
        global _cat
        _cat = subprocess.Popen(['git', '-C', str(self._repo.git_dir), 'cat-file', '--batch'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def tree_entries(self, data):
        while len(data):
            offs = data.find(b'\x00') + 21
            yield data[:offs]
            data = data[offs:]

    def get_obj(self, x):
        binsha = unhexlify(x[0])

        _cat.stdin.write(x[0]+b'\n')
        _cat.stdin.flush()
        line = _cat.stdout.readline().strip().split()
        #print(line)
        data = _cat.stdout.read(int(line[2], 10)+1)

        if x[1] == b'commit':
            lines = data.split(b'\n')
            tree = unhexlify(lines[0][5:45])
            parents = []
            for line in lines[1:]:
                if line.startswith(b'parent '):
                    parents.append(unhexlify(line[7:47]))
                else:
                    break

            return x[1], binsha, (tree, parents)

        elif x[1] == b'tree':
            trees = []
            blobs = []
            for entry in self.tree_entries(data[:-1]):
                o_binsha = entry[-20:]
                if entry[5] == 32:
                    trees.append(o_binsha)
                elif entry[6] == 32:
                    blobs.append(o_binsha)
                else:
                    print(entry[5], entry[6])
            return x[1], binsha, (trees, blobs)

        else:
            return x[1], binsha, None



class CatFile(object):
    """Multithreaded cat-file."""

    def __init__(self, repo, iterable, threads=None):
        if threads is None:
            self._threads = multiprocessing.cpu_count()
        else:
            self._threads = threads
        self._repo = repo
        self._it = iterable
        self._thread = CatFileThread(self._repo)

    def __enter__(self):
        if self._threads > 1:
            multiprocessing.set_start_method('spawn')
            self._pool = multiprocessing.Pool(processes=self._threads, initializer=self._thread.initializer)
        return self

    def __exit__(self, *args):
        if self._threads > 1:
            self._pool.terminate()

    def __iter__(self):
        if self._threads > 1:
            return self._pool.imap_unordered(self._thread.get_obj, self._it, chunksize=5000)
        else:
            self._thread.initializer()
            return map(self._thread.get_obj, self._it)