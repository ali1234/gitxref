import hashlib
from collections import defaultdict

import numpy as np
from tqdm import tqdm


def hashblob(f):
    h = hashlib.sha1()
    h.update(b'blob %d\0' % f.stat().st_size)
    h.update(f.read_bytes())
    return h.digest()


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
        extra = len(self.blobs)%8
        if extra: # zero the unused bits of unfound
            for i in range(extra,8):
                unfound[-1] -= 128>>i

        keyfunc = lambda x: np.sum(np.unpackbits((x[1]&unfound)))

        commit_sets = defaultdict(list)
        for commit, array in tqdm(self.commits.items(), unit=' commits', desc='Grouping commits'):
            commit_sets[array.tobytes()].append(commit)

        best = sorted(((v[0], np.frombuffer(k, dtype=np.uint8)) for k, v in commit_sets.items()), key=keyfunc)

        with tqdm(total=len(self.blobs), unit=' blobs', desc='Finding best commits') as pbar:
            while len(best):
                inbest = best[0][1]&unfound
                pbar.update(np.sum(np.unpackbits(inbest)))
                yield (best[0][0], inbest)
                unfound &= ~best[0][1]
                best = list(filter(lambda x: keyfunc(x) > 0, best[1:]))
                best.sort(key=keyfunc, reverse=True)
            pbar.update(np.sum(np.unpackbits(unfound)))
            yield (None, unfound)

    def __getitem__(self, arg):
        if type(arg) == int:
            return (self.blobs[arg], self.paths[self.blobs[arg]])
        else:
            arg = np.unpackbits(arg)
            yield from ((self.paths[self.blobs[x]], self.blobs[x]) for x,t in enumerate(arg[:len(self.blobs)]) if t)
