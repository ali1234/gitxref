from binascii import unhexlify

from gitxref.batchcheck import BatchCheck


class Batch(BatchCheck):

    """
    Extends the BatchCheck pipeline with the following:

         | git cat-file --buffer --batch

    and then parses the results into objects.
    """

    def __init__(self, repo, types=None):
        super().__init__(repo, types=types)
        self._pipeline.append(self.git_base + ['--batch'])

    def tree_entries(self, data):
        last = 0
        while True:
            offs = data.find(b'\x00', last)
            if offs < 0:
                break
            yield data[last:offs], data[offs+1:offs+21]
            last = offs + 21

    def __iter__(self):

        while True:
            header = self._datapipe.readline().strip().split()
            if len(header) == 0:
                return
            data = self._datapipe.read(int(header[2], 10))
            self._datapipe.read(1)
            if header[1] == b'commit':
                lines = data.split(b'\n')
                tree = unhexlify(lines[0][5:45])
                parents = []
                for line in lines[1:]:
                    if line.startswith(b'parent '):
                        parents.append(unhexlify(line[7:47]))
                    else:
                        break

                yield b'commit', unhexlify(header[0]), (tree, parents)

            elif header[1] == b'tree':
                trees = []
                blobs = []
                for entry, o_binsha in self.tree_entries(data):
                    if entry[5] == 32:
                        trees.append(o_binsha)
                    elif entry[6] == 32:
                        blobs.append(o_binsha)
                    else:
                        print(entry[5], entry[6])
                yield b'tree', unhexlify(header[0]), (trees, blobs)

            else:
                yield header[1], unhexlify(header[0]), None
