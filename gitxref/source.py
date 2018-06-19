import binascii
import hashlib

from tqdm import tqdm


class Blob(object):

    def __init__(self, f):
        h = hashlib.sha1()
        h.update(b'blob %d\0' % f.stat().st_size)
        h.update(f.read_bytes())
        self._binsha = h.digest()
        self.paths = set()

    def __hash__(self):
        return hash(self._binsha)

    def __eq__(self, other):
        try:
            return self._binsha == other._binsha
        except AttributeError:
            return self._binsha == other

    def __str__(self):
        return binascii.hexlify(self._binsha).decode('utf8')

    @property
    def binsha(self):
        return self._binsha


class Source(object):

    def __init__(self, root):
        self._root = root
        self._blobs = []
        self._blob_index = {}

        files = list(root.rglob('*'))

        for f in tqdm(files, unit=' blobs', desc='Hashing blobs'):
            path = f.relative_to(root)
            if f.is_file() and not f.is_symlink():
                b = Blob(f)
                try:
                    self._blob_index[b].paths.add(path)
                except KeyError:
                    b.paths.add(path)
                    self._blob_index[b] = b

        self._blobs = list(self._blob_index.keys())

    def __contains__(self, item):
        return item in self._blob_index

    def __iter__(self):
        return (b.binsha for b in self._blobs)

    def __len__(self):
        return len(self._blobs)

    def __getitem__(self, item):
        try:
            return self._blobs[item]
        except TypeError:
            return self._blob_index[item]

