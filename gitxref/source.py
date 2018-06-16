import hashlib

import numpy as np
from bitarray import bitarray


def hashblob(f):
    h = hashlib.sha1()
    h.update(b'blob %d\0' % f.stat().st_size)
    h.update(f.read_bytes())
    return h.digest()


def andcount(a, b):
    return np.sum(np.unpackbits((a&b)))


class Source(object):

    def __init__(self, repo, directory):
        self.repo = repo
        self.directory = directory
        self.blobs = set()
        self.paths = {}

        for f in directory.rglob('*'):
            path = f.relative_to(directory)
            if f.is_file() and not f.is_symlink():
                binsha = hashblob(f)
                self.blobs.add(binsha)
                self.paths[binsha] = str(path)

        self.blobs = list(self.blobs)

        self.blob_index = {k:v for v,k in enumerate(self.blobs)}

    def make_bitmaps(self, graph):
        self.commits = graph.make_bitmaps(self.blobs)

    def find_best(self):
        unfound = np.empty(((len(self.blobs)+7)//8,), dtype=np.uint8)
        unfound[:] = 0xff

        keyfunc = lambda x: np.sum(np.unpackbits((x[1]&unfound)))

        best = sorted(self.commits.items(), key=keyfunc, reverse=True)

        while len(best):
            yield (best[0][0], best[0][1]&unfound)
            unfound &= ~best[0][1]
            best = list(filter(lambda x: keyfunc(x) > 0, best[1:]))
            best.sort(key=keyfunc, reverse=True)

        yield (None, unfound)
        return

    def __getitem__(self, arg):
        if type(arg) == int:
            return (self.blobs[arg], self.paths[self.blobs[arg]])
        else:
            arg = np.unpackbits(arg)
            yield from ((self.blobs[x], self.paths[self.blobs[x]]) for x,t in enumerate(arg[:len(self.blobs)]) if t)
