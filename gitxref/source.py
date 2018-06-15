import hashlib

from bitarray import bitarray


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
        unfound = bitarray(len(self.blobs))
        unfound.setall(1)

        best = sorted(self.commits.items(), key=lambda x: (x[1]&unfound).count(), reverse=True)

        while len(best):
            yield (best[0][0], best[0][1]&unfound)
            unfound &= ~best[0][1]

            # old way - evaluates sum(x[1]&unfound) twice
            best = list(filter(lambda x: sum(x[1]&unfound) > 0, best[1:]))
            best.sort(key=lambda x: (x[1]&unfound).count(), reverse=True)

            # new
            #d = ((sum(x[1]&unfound), x) for x in best[1:])
            #s = sorted(filter(lambda x: x[0] > 0, d))
            #best = [x[1] for x in s]




        yield (None, unfound)
        return

    def __getitem__(self, arg):
        if type(arg) == int:
            return (self.blobs[arg], self.paths[self.blobs[arg]])
        else:
            yield from ((self.blobs[x], self.paths[self.blobs[x]]) for x,t in enumerate(arg[:len(self.blobs)]) if t)
