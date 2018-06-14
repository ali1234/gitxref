import hashlib
import itertools
import pickle
from collections import defaultdict

from tqdm import tqdm

from gitxref.cache import Cache


def list_opt(l):
    count = 0
    for n in range(len(l)):
        while type(l[n]) is list and len(l[n]) == 1:
            l[n] = l[n][0]
            count += 1
    return count


class Backrefs(object):

    """
    Implements a reverse mapping of blobs to the objects which contain them.

    For each object in the repo, add the object's binsha to each of its child
    object's sets.

    The back references are cached and invalidated if for_each_ref changed.
    """

    def __init__(self, repo, skip_cache=False, rebuild=False):
        self.repo = repo
        self._cache = Cache(self.repo)

        if skip_cache or rebuild:
            data = self.generate()
            if not skip_cache:
                self._cache['backrefs'] = data
        else:
            try:
                data = self._cache['backrefs']
            except KeyError:
                data = self.generate()
                self._cache['backrefs'] = data

        self.backrefs, self.commit_parents = data

    def generate(self):
        """
        Regenerates the backrefs from the repo data.
        """
        backrefs = defaultdict(list)
        trees = defaultdict(list)
        commit_parents = defaultdict(list)
        typecount = defaultdict(int)

        for obj_type, obj_binsha, x in tqdm(self.repo.objects, unit=' objects', desc='Scanning repository metadata'):
            typecount[obj_type] += 1

            if obj_type == b'commit':
                trees[x[0]].append(obj_binsha)
                for binsha in x[1]:
                    commit_parents[obj_binsha].append(binsha)

            elif obj_type == b'tree':
                for binsha in x[0]:
                    trees[binsha].append(trees[obj_binsha])
                for binsha in x[1]:
                    backrefs[binsha].append(trees[obj_binsha])

        print(', '.join('{:s}s: {:d}'.format(k.decode('utf8').capitalize(), v) for k,v in typecount.items()))

        count = sum(list_opt(v) for v in tqdm(trees.values(), unit=' trees', desc='Optimizing trees'))
        print(count, 'singular references removed.')
        count = sum(list_opt(v) for v in tqdm(backrefs.values(), unit=' blobs', desc='Optimizing blobs'))
        print(count, 'singular references removed.')

        #backrefs = dict((k, v[0] if len(v) == 1 else v) for k, v in tqdm(backrefs.items(), unit=' blobs', desc='Getting blob trees'))

        return backrefs, commit_parents

    def commits_fetch(self, l):
        for i in l:
            if type(i) is bytes:
                yield i
            else:
                yield from self.commits_fetch(i)

    def commits_for_object(self, binsha):
        """
        Gets all the commits containing a blob.
        """
        if binsha in self.backrefs:
            return self.commits_fetch(self.backrefs[binsha])
        else:
            return []
