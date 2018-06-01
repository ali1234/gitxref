import hashlib
from collections import defaultdict


def hashblob(f):
    h = hashlib.sha1()
    h.update(b'blob %d\0' % f.stat().st_size)
    h.update(f.read_bytes())
    return h.digest()


class Source(object):

    def __init__(self, blobs, backrefs):
        self.blobs = blobs
        self.backrefs = backrefs
        self.best_commit_for_blob = {}

    @classmethod
    def scan(cls, directory, backrefs):

        blobs = {}
        for f in directory.rglob('*'):

            path = f.relative_to(directory)
            if f.is_symlink():
                pass
            elif f.is_file():
                binsha = hashblob(f)
                blobs[path] = binsha

        return Source(blobs, backrefs)

    def find_backrefs(self):
        commits_for_blob = {}
        blobs_for_commit = defaultdict(set)
        all_commits = set()

        for path, binsha in self.blobs.items():
            if binsha in self.best_commit_for_blob:
                continue
            if self.backrefs.has_object(binsha):
                commits = set(self.backrefs.commits_for_object(binsha))
                if len(commits) == 0:
                    print("WARNING NO COMMITS")
                commits_for_blob[binsha] = commits
                for c in commits:
                    all_commits.add(c)
                    blobs_for_commit[c].add(binsha)

        if len(blobs_for_commit) == 0:
            return False

        self.commits = sorted([(len(v), k) for k, v in blobs_for_commit.items()], reverse=True)

        best_blobs = blobs_for_commit[self.commits[0][1]]

        for blob in best_blobs:
            self.best_commit_for_blob[blob] = self.commits[0][1]

        return True



