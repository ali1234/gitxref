import hashlib
import pathlib
import pickle
from collections import defaultdict

import git
from git.repo.fun import name_to_object


class Backrefs(object):

    """
    Implements a reverse mapping of blobs to the objects which contain them.

    For each object in the repo, add the object's binsha to each of its child
    object's sets.

    The back references are cached and invalidated if for_each_ref changed.
    """

    def __init__(self, repo):
        self.repo = repo
        self.gitdir = pathlib.Path(repo.git.rev_parse('--absolute-git-dir').strip())
        self.backrefs = self.load()


    def check(self):
        """
        Check the hash of git for_each_rev. If this changed, the cache is invalid.
        """
        return hashlib.sha1(self.repo.git.for_each_ref().encode('utf8')).digest()


    def load(self):
        """
        Attempts to load the backrefs from the cache, or regenerate it if there is
        a problem.
        """
        hash = self.check()
        check_file = (self.gitdir / 'backrefs.checksum')
        backrefs_file = (self.gitdir / 'backrefs.pickle')
        if check_file.is_file():
            old = check_file.read_bytes()
            if old == hash:
                try:
                    with backrefs_file.open('rb') as f:
                        return pickle.load(f)
                except:
                    pass

        backrefs = self.generate()
        with backrefs_file.open('wb') as f:
            pickle.dump(backrefs, f)
        check_file.write_bytes(hash)
        return backrefs


    def generate(self):
        """
        Regenerates the backrefs from the repo data.
        """
        print('Regenerating backrefs database. This may take a few minutes.')

        backrefs = defaultdict(set)

        for o in self.repo.git.rev_list('--objects', '-g', '--no-walk', '--all').split('\n'):
            name = o.split()[0]
            obj = name_to_object(self.repo, name)
            # print(type(obj))
            if type(obj) == git.objects.tree.Tree:
                obj.path = 'unknown'  # https://github.com/gitpython-developers/GitPython/issues/759
                for t in obj.trees:
                    backrefs[t.binsha].add(obj.binsha)
                for b in obj.blobs:
                    backrefs[b.binsha].add(obj.binsha)
            elif type(obj) == git.objects.commit.Commit:
                backrefs[obj.tree.binsha].add(obj.binsha)

        return backrefs


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