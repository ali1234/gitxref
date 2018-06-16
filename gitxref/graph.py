from collections import defaultdict

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

        return blobs

    def _topo_visit(self, vertex, result_list, visited_set):
        if vertex in visited_set:
            return
        if type(vertex) is Vertex:
            for v in vertex:
                self._topo_visit(v, result_list, visited_set)
        visited_set.add(vertex)
        result_list.append(vertex)

    def topo_sort(self, sources):
        """Topo sorts the graph vertices reachable from sources."""
        visited_set = set()
        result_list = list()

        for v in tqdm(sources, unit=' sources', desc='Topological sort'):
            self._topo_visit(self.blobs[v], result_list, visited_set)
        return result_list[::-1]

    def make_bitmaps(self, blobs, step=None):
        if step is None:
            step = len(blobs)
        elif step % 8:
            raise ValueError('step must be a multiple of 8 or None.')
        b_step = (step+7)//8

        commits = defaultdict(lambda: np.zeros(((len(blobs)+7)//8,), dtype=np.uint8))

        for i in range(0, len(blobs), step):
            b_i = i//8
            topo = self.topo_sort(blobs[i:i+step])
            for n, v in enumerate(blobs[i:i+step]):
                self.blobs[v].bitmap = np.zeros((b_step,), dtype=np.uint8)
                self.blobs[v].bitmap[n//8] = 128>>(n%8)

            for v in tqdm(topo, unit=' vertices', desc='Making bitmaps'):
                if type(v) is not Vertex:
                    continue
                for vv in v:
                    if type(vv) is Vertex:
                        try:
                            vv.bitmap |= v.bitmap
                        except AttributeError:
                            vv.bitmap = v.bitmap.copy()
                    else:
                        commits[vv][b_i:b_i+b_step] |= v.bitmap
                del v.bitmap

        return commits