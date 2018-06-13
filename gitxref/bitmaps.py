import multiprocessing
import subprocess
from binascii import unhexlify

from bitarray import bitarray
from tqdm import tqdm

from gitxref.batchcheck import BatchCheck


class BitmapThread(object):
    def __init__(self, blob_index, path):
        self.blob_index = blob_index
        self.path = path

    def build(self, commit):
        bitmap = bitarray(len(self.blob_index), endian='big')
        bitmap[:] = False
        tree = subprocess.check_output(['git', '-C', self.path, 'ls-tree', '-r', commit]).split(b'\n')
        for line in tree:
            binsha = unhexlify(line[12:52])
            try:
                bitmap[self.blob_index[binsha]] = True
            except KeyError:
                pass
        return unhexlify(commit), bitmap


class Bitmaps(object):

    def __init__(self, repo, processes=None):
        if processes is None:
            self._processes = multiprocessing.cpu_count()
        else:
            self._processes = processes
        self._repo = repo

    def __enter__(self):
        if self._processes > 0:
            #multiprocessing.set_start_method('spawn')
            self._pool = multiprocessing.Pool(processes=self._processes)

        return self

    def __exit__(self, *args):
        if self._processes > 0:
            self._pool.terminate()

    def build(self, blob_index):
        with BatchCheck(self._repo, types=['c']) as commits:
            _thread = BitmapThread(blob_index, self._repo.git_dir)
            if self._processes > 0:
                itor = self._pool.imap_unordered(_thread.build, commits, chunksize=1000)
            else:
                itor = map(_thread.build, commits)
            return dict(tqdm(itor, unit='commit'))
