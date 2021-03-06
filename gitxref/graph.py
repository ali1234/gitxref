import sys
from collections import defaultdict

import numpy as np
from tqdm import tqdm


class Vertex(list):
    """
    This is exactly like a list, but hashable and equal only to itself.
    """
    __slots__ = ('bitmap',)

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return id(self) == id(other)

    def reduce(self, visited_set):
        for n, v in enumerate(self):
            if type(v) is Vertex:
                if v not in visited_set:
                    v.reduce(visited_set)
                if len(v) == 1:
                    self[n] = v[0]
        visited_set.add(self)

    def topo_visit(self, result_list, visited_set):
        for v in self:
            if type(v) is Vertex and v not in visited_set:
                v.topo_visit(result_list, visited_set)
        visited_set.add(self)
        result_list.append(self)


class Graph(object):

    def __init__(self, repo):
        self._repo = repo

        if 'graph' in repo.cache:
            data = self._repo.cache['graph']
        else:
            data = self._generate()
            self._repo.cache['graph'] = data

        self.blobs = data

    def __contains__(self, item):
        return item in self.blobs

    def _generate(self):
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

        print(', '.join('{:s}s: {:d}'.format(k.decode('utf8').capitalize(), v) for k,v in typecount.items()), file=sys.stderr)
        print('Blobs:', len(blobs), 'Edges:', edges, file=sys.stderr)

        visited_set = set()
        for v in tqdm(blobs.values(), unit=' blobs', desc='Reducing graph'):
            v.reduce(visited_set)

        return dict(blobs)

    def topo_sort(self, sources):
        """Topo sorts the graph vertices reachable from sources."""
        visited_set = set()
        result_list = list()

        for v in tqdm(sources, unit=' sources', desc='Topological sort'):
            if v in self.blobs:
                self.blobs[v].topo_visit(result_list, visited_set)
        return result_list[::-1]

    def bitmaps(self, source, step=None):
        """Returns a dict of commit binsha => bitmap."""
        if step is None or step > len(source):
            step = len(source)
        elif step % 8:
            raise ValueError('step must be a multiple of 8 or None.')

        bitmaps = defaultdict(lambda: np.zeros(((len(source) + 7) // 8,), dtype=np.uint8))

        step_range = range(0, len(source), step)
        if len(step_range) > 1:
            step_range = tqdm(step_range, unit=' steps', desc='Making bitmaps')

        for i in step_range:
            b_step = (min(len(source) - i, step) + 7) // 8
            b_i = i//8
            topo = self.topo_sort(source[i:i + step])
            for n, v in enumerate(source[i:i + step]):
                if v in self.blobs:
                    self.blobs[v].bitmap = np.zeros((b_step,), dtype=np.uint8)
                    self.blobs[v].bitmap[n//8] = 128>>(n%8)

            for v in tqdm(topo, unit=' vertices', desc='Pushing sub-bitmaps'):
                for vv in v:
                    if type(vv) is Vertex:
                        try:
                            vv.bitmap |= v.bitmap
                        except AttributeError:
                            vv.bitmap = v.bitmap.copy()
                    else:
                        bitmaps[vv][b_i:b_i+b_step] |= v.bitmap
                del v.bitmap

        return dict(bitmaps)
