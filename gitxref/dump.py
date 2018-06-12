import subprocess

from binascii import unhexlify


class Dump(object):

    """
    Implements the following shell pipeline to dump objects:

        cat-file --buffer --batch-check='%(objecttype) %(objectname)' --batch-all-objects | grep -E \(^t\|^c\) | cut -d \  -f 2 | git cat-file --buffer --batch
    """

    def __init__(self, repo):
        self._repo = repo

    def __enter__(self):
        git_base = ['git', '-C', str(self._repo.git_dir), 'cat-file', '--buffer']

        pipeline = [
            git_base + ['--batch-check=%(objecttype) %(objectname)', '--batch-all-objects'],
            ['grep', '-E', '(^c|^t)'],
            ['cut', '-d', ' ', '-f', '2'],
            git_base + ['--batch']
        ]

        self._datapipe = None
        self._procs = []

        for p in pipeline:
            proc = subprocess.Popen(p, stdin=self._datapipe, stdout=subprocess.PIPE)
            self._datapipe = proc.stdout
            self._procs.append(proc)

        return self

    def __exit__(self, *args):
        for p in self._procs:
            p.terminate()
        for p in self._procs:
            p.wait(timeout=3)
        for p in self._procs:
            p.kill()
        for p in self._procs:
            p.wait()

    def tree_entries(self, data):
        last = 0
        while True:
            offs = data.find(b'\x00', last)
            if offs < 0:
                break
            yield data[last:offs], data[offs+1:offs+21]
            last = offs + 21

    def objects(self):

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
