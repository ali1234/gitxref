import hashlib
import itertools
import pathlib
import pickle
from collections import defaultdict

import git
from git.repo.fun import name_to_object

from gitxref.dedup import Dedup
from gitxref.util import b2h


class Backrefs(object):

    """
    Implements a reverse mapping of blobs to the objects which contain them.

    For each object in the repo, add the object's binsha to each of its child
    object's sets.

    The back references are cached and invalidated if for_each_ref changed.
    """

    def __init__(self, repo, rebuild=False):
        self.repo = repo
        self.gitdir = pathlib.Path(repo.git.rev_parse('--absolute-git-dir').strip())
        self.backrefs,self.commit_parents = self.load(rebuild)


    def check(self):
        """
        Check the hash of git for_each_rev. If this changed, the cache is invalid.
        """
        return hashlib.sha1(self.repo.git.for_each_ref().encode('utf8')).digest()


    def load(self, rebuild=False):
        """
        Attempts to load the backrefs from the cache, or regenerate it if there is
        a problem.
        """
        hash = self.check()
        check_file = (self.gitdir / 'backrefs.checksum')
        backrefs_file = (self.gitdir / 'backrefs.pickle')
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

        backrefs = defaultdict(set)
        commit_parents = defaultdict(set)

        seen = Dedup()
        for o in self.repo.git.rev_list('--objects', '--all').split('\n'):
            name = o.split()[0]
            obj = name_to_object(self.repo, name)
            obj_binsha = seen[obj.binsha]
            if type(obj) == git.objects.tree.Tree:
                obj.path = 'unknown'  # https://github.com/gitpython-developers/GitPython/issues/759
                for o in itertools.chain(obj.trees, obj.blobs):
                    backrefs[seen[o.binsha]].add(obj_binsha)
            elif type(obj) == git.objects.commit.Commit:
                backrefs[seen[obj.tree.binsha]].add(obj_binsha)
                for p in obj.parents:
                    commit_parents[obj_binsha].add(seen[p.binsha])

        print('Unique binsha:', len(seen), 'Duplicates:', seen.eliminated())

        seen = Dedup()
        for k, v in backrefs.items():
            backrefs[k] = seen[frozenset(v)]

        print('Unique sets:', len(seen), 'Duplicates:', seen.eliminated())

        return backrefs, commit_parents

    def has_object(self, binsha):
        """
        Checks if the given binsha is known in the backrefs database.
        """
        return binsha in self.backrefs


    def commits_for_object(self, binsha):
        """
        Gets all the commits containing an object. Commits aren't contained by anything,
        so you found one when there is no further iteration.
        """
        roots = self.backrefs[binsha]
        for binsha_next in roots:
            if binsha_next in self.backrefs:
                yield from self.commits_for_object(binsha_next)
            else:
                yield binsha_next

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