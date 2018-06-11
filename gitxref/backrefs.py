import hashlib
import pickle
from collections import defaultdict

from gitxref.util import b2h


class Backrefs(object):

    """
    Implements a reverse mapping of blobs to the objects which contain them.

    For each object in the repo, add the object's binsha to each of its child
    object's sets.

    The back references are cached and invalidated if for_each_ref changed.
    """

    def __init__(self, repo, skip_cache=False, rebuild=False, threads=1):
        self.repo = repo
        self.threads = threads
        if skip_cache:
            self.backrefs, self.commit_parents = self.generate()
        else:
            self.backrefs,self.commit_parents = self.load(rebuild)

    def check(self):
        """
        Check the hash of git for_each_ref. If this changed, the cache is invalid.
        """
        return hashlib.sha1(self.repo.git.for_each_ref()).digest()

    def load(self, rebuild=False):
        """
        Attempts to load the backrefs from the cache, or regenerate it if there is
        a problem.
        """
        hash = self.check()
        check_file = (self.repo.git_dir / 'backrefs.checksum')
        backrefs_file = (self.repo.git_dir / 'backrefs.pickle')
        if not rebuild:
            if check_file.is_file():
                old = check_file.read_bytes()
                if old == hash:
                    try:
                        with backrefs_file.open('rb') as f:
                            return pickle.load(f)
                    except:
                        pass

        db = self.generate()
        with backrefs_file.open('wb') as f:
            pickle.dump(db, f)
        check_file.write_bytes(hash)
        return db

    def generate(self):
        """
        Regenerates the backrefs from the repo data.
        """
        print('Regenerating backrefs database. This may take a few minutes.')

        backrefs = defaultdict(list)
        trees = defaultdict(list)
        commit_parents = defaultdict(list)
        typecount = defaultdict(int)

        for obj_type, obj_binsha, x in self.repo.objects:
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

        return backrefs, commit_parents

    def __contains__(self, binsha):
        """
        Checks if the given binsha is known in the backrefs database.
        """
        return binsha in self.backrefs

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
            yield from self.commits_fetch(self.backrefs[binsha])

    def all_parents(self, binsha, binshas=None):
        if binshas is None or binsha in binshas:
            yield binsha
        for p in self.commit_parents[binsha]:
            yield from self.all_parents(p, binshas)

    def root_commits(self, binsha):
        print(b2h(binsha), self.commit_parents[binsha])
        if len(self.commit_parents[binsha]) == 0:
            yield binsha
        else:
            for p in self.commit_parents[binsha]:
                yield from self.root_commits(p)