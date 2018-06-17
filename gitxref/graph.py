from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import numpy as np
from tqdm import tqdm

from gitxref.cache import Cache


class Vertex(list):
    """
    This is exactly like a list, but hashable and equal only to itself.
    """
    __slots__ = ('bitmap')

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return id(self) == id(other)

    def reduce(self):
        for n in range(len(self)):
            while type(self[n]) is Vertex and len(self[n]) == 1:
                self[n] = self[n][0]


class Graph(object):

    def __init__(self, repo, skip_cache=False, rebuild=False):
        self._repo = repo
        self._cache = Cache(self._repo)

        if skip_cache or rebuild:
            data = self.generate()
            if not skip_cache:
                self._cache['graph'] = data
        else:
            try:
                data = self._cache['graph']
            except KeyError:
                data = self.generate()
                self._cache['graph'] = data

        self.blobs = data

    def generate(self):
        blobs = defaultdict(Vertex)
        trees = defaultdict(Vertex)
        typecount = defaultdict(int)
        edges = 0

        for obj_type, obj_binsha, x in tqdm(self._repo.objects, unit=' objects', desc='Building reversed graph'):
            typecount[obj_type] += 1

            if obj_type == b'commit':
                trees[x[0]].append(obj_binsha)
                edges += 1

            elif obj_type == b'tree':
                for binsha in x[0]:
                    trees[binsha].append(trees[obj_binsha])
                for binsha in x[1]:
                    blobs[binsha].append(trees[obj_binsha])
                edges += len(x[0]) + len(x[1])

        print(', '.join('{:s}s: {:d}'.format(k.decode('utf8').capitalize(), v) for k,v in typecount.items()))
        print('Blobs:', len(blobs), 'Edges:', edges)

        for v in tqdm(trees.values(), unit=' trees', desc='Reducing trees'):
            v.reduce()
        for v in tqdm(blobs.values(), unit=' blobs', desc='Reducing blobs'):
            v.reduce()

        return dict(blobs)

    def _topo_visit(self, vertex, result_list, visited_set):
        if type(vertex) is not Vertex or vertex in visited_set:
            return
        for v in vertex:
            self._topo_visit(v, result_list, visited_set)
        visited_set.add(vertex)
        result_list.append(vertex)

    def topo_sort(self, sources):
        """Topo sorts the graph vertices reachable from sources."""
        visited_set = set()
        result_list = list()

        for v in tqdm(sources, unit=' sources', desc='Topological sort'):
            if v in self.blobs:
                self._topo_visit(self.blobs[v], result_list, visited_set)
        return result_list[::-1]

    def bitmaps(self, blobs, step=None):
        """Returns an iter yielding (commit, bitmap) tuples."""
        if step is None:
            step = len(blobs)
        elif step % 8:
            raise ValueError('step must be a multiple of 8 or None.')

        commits = defaultdict(lambda: np.zeros(((len(blobs)+7)//8,), dtype=np.uint8))

        for i in range(0, len(blobs), step):
            b_step = (min(len(blobs)-i, step) + 7) // 8
            b_i = i//8
            topo = self.topo_sort(blobs[i:i+step])
            for n, v in enumerate(blobs[i:i+step]):
                if v in self.blobs:
                    self.blobs[v].bitmap = np.zeros((b_step,), dtype=np.uint8)
                    self.blobs[v].bitmap[n//8] = 128>>(n%8)

            for v in tqdm(topo, unit=' vertices', desc='Making bitmaps'):
                for vv in v:
                    if type(vv) is Vertex:
                        try:
                            vv.bitmap |= v.bitmap
                        except AttributeError:
                            vv.bitmap = v.bitmap.copy()
                    else:
                        commits[vv][b_i:b_i+b_step] |= v.bitmap
                del v.bitmap

        # group all found commits with the same bitmap.
        commit_groups = defaultdict(list)
        for commit, array in tqdm(commits.items(), unit=' commits', desc='Grouping commits'):
            commit_groups[array.tobytes()].append(commit)

        return list((v, np.frombuffer(k, dtype=np.uint8)) for k, v in commit_groups.items())
