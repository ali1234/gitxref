from bitarray import bitarray

from gitxref.util import b2h, bitarray_defaultdict, hashblob, bitarray_zero


class Source(object):

    def __init__(self, directory, backrefs):
        self.directory = directory
        self.backrefs = backrefs
        self.blobs = []
        self.paths = []

        for f in directory.rglob('*'):
            path = f.relative_to(directory)
            if f.is_file() and not f.is_symlink():
                self.blobs.append(hashblob(f))
                self.paths.append(path)

        self.blob_index = {k:v for v,k in enumerate(self.blobs)}
        self.path_index = {k:v for v,k in enumerate(self.paths)}

    def find_backrefs(self):
        count = 0
        self.commits = bitarray_defaultdict(len(self.blobs))
        for index, binsha in enumerate(self.blobs):
            if binsha in self.backrefs:
                for c in self.backrefs.commits_for_object(binsha):
                    self.commits[c][index] = True
            count += 1
            print('Blobs checked: {:6d}/{:d} Commits seen: {:12d}'.format(count, len(self.blobs), len(self.commits)), self.paths[index])

    def find_best(self):
        best = self.commits.items()
        unfound = bitarray(len(self.blobs))
        unfound[:] = True

        while True:
            best = sorted(best, key=lambda x: sum(x[1]&unfound), reverse=True)
            count = sum(best[0][1]&unfound)
            if count == 0:
                return
            yield (best[0][0], best[0][1]&unfound)
            unfound &= ~best[0][1]
            best = best[1:]

    def __getitem__(self, arg):
        if type(arg) == int:
            return (self.blobs[arg], self.paths[arg])
        else:
            yield from ((self.blobs[x], self.paths[x]) for x,t in enumerate(arg) if t)
