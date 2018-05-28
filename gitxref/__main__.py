import argparse
import binascii
import hashlib
import itertools
import json
import pickle
import subprocess
import pathlib
from collections import defaultdict
from datetime import datetime

import git
from git.repo.fun import name_to_object


def find_backrefs(repo):
    return repo.git.rev_parse('--absolute-git-dir').strip() + '/backrefs.pickle'


def generate_backrefs(repo):
    print('Regenerating backref database. This may take a few minutes.')

    backrefs = defaultdict(set)

    for o in repo.git.rev_list('--objects', '-g', '--no-walk', '--all').split('\n'):
        name = o.split()[0]
        obj = name_to_object(repo, name)
        # print(type(obj))
        if type(obj) == git.objects.tree.Tree:
            obj.path = 'unknown' # https://github.com/gitpython-developers/GitPython/issues/759
            for t in obj.trees:
                backrefs[t.binsha].add(obj.binsha)
            for b in obj.blobs:
                backrefs[b.binsha].add(obj.binsha)
        elif type(obj) == git.objects.commit.Commit:
            backrefs[obj.tree.binsha].add(obj.binsha)

    with open(find_backrefs(repo), 'wb') as f:
        pickle.dump(backrefs, f)

    return backrefs


def load_backrefs(repo):
    try:
        with open(find_backrefs(repo), 'rb') as f:
            return pickle.load(f)
    except:
        return generate_backrefs(repo)


def hashblob(f):
    h = hashlib.sha1()
    h.update(b'blob %d\0' % f.stat().st_size)
    h.update(f.read_bytes())
    return h.digest()


def get_commits(backrefs, binsha):
    parents = backrefs[binsha]
    for binsha_next in parents:
        if binsha_next in backrefs:
            yield from get_commits(backrefs, binsha_next)
        else:
            yield binsha_next


def main():

    parser = argparse.ArgumentParser(description='Git x ref.')
    parser.add_argument('directory', metavar='directory', type=pathlib.Path,
                        help='Path to unpacked tarball.')
    parser.add_argument('repository', metavar='repository', type=pathlib.Path,
                        help='Path to Git repository.')

    args = parser.parse_args()

    repo = git.Repo(str(args.repository))
    backrefs = load_backrefs(repo)

    exact = 0
    unknown = 0

    commits_for_blob = {}
    blobs_for_commit = defaultdict(set)
    unique_paths_for_commit = defaultdict(set)
    blobs = set()
    exact_blobs = set()
    unknown_blobs = set()

    for f in args.directory.rglob('*'):

        path = f.relative_to(args.directory)
        if f.is_symlink():
            pass
        elif f.is_file():
            binsha = hashblob(f)
            blobs.add(binsha)
            if binsha in backrefs:
                exact += 1
                exact_blobs.add(binsha)
                commits = set(get_commits(backrefs, binsha))
                if len(commits) == 0:
                    print("WARNING NO COMMITS")
                if len(commits) == 1:
                    unique_paths_for_commit[list(commits)[0]].add(path)
                commits_for_blob[binsha] = commits
                for c in commits:
                    commits.add(c)
                    blobs_for_commit[c].add(binsha)
            else:
                unknown_blobs.add(binsha)

    commit_list = []

    for k,v in blobs_for_commit.items():
        commit_list.append((len(v),k))

    commit_list.sort(reverse=True)

    print('Total blobs:', len(blobs), 'Found:', len(exact_blobs), 'Unfound:', len(unknown_blobs))

    commit_list_filtered = []
    filtered_count = 0
    while len(commit_list):
        last = commit_list.pop()
        if any(blobs_for_commit[last[1]].issubset(blobs_for_commit[s[1]]) for s in commit_list):
            filtered_count += 1
        else:
            commit_list_filtered.insert(0, last)


    required_commits = []
    optional_commits = []
    for c in commit_list_filtered:
        if blobs_for_commit[c[1]].issubset(set.union(*(blobs_for_commit[cc[1]] for cc in commit_list_filtered if cc != c))):
            optional_commits.append(c)
        else:
            required_commits.append(c)

    required_union = set.union(*(blobs_for_commit[cc[1]] for cc in required_commits))

    optional_commits_filtered = []
    optional_filtered_count = 0
    for c in optional_commits:
        if blobs_for_commit[c[1]].issubset(required_union):
            optional_filtered_count += 1
        else:
            optional_commits_filtered.append(c)

    print('')
    print(filtered_count, 'commits filtered as direct subsets.')
    print(len(required_commits), 'required commits.')
    print(optional_filtered_count, 'commits filtered as subsets of required set.')
    print(len(optional_commits_filtered), 'optional commits.')

    print('')
    print('Required commits by number of unique blobs:')

    for v,k in sorted(required_commits, reverse=True, key=lambda x: len(unique_paths_for_commit[x[1]])):
        print(v, binascii.hexlify(k).decode('utf8'))
        print('   ', len(unique_paths_for_commit[k]), 'matching blob paths unique to this commit:')
        for path in unique_paths_for_commit[k]:
            print('       ', path)

    print('')
    print('Smallest combination of optional commits:')

    found = False
    for nc in range(1, len(optional_commits_filtered)):
        for commits in itertools.combinations(optional_commits_filtered, nc+1):
            union = set.union(*(blobs_for_commit[c[1]] for c in commits)).union(required_union)
            if len(union) == len(exact_blobs): #commit_list[0][0]:
                for v, k in commits:
                    print(v, binascii.hexlify(k).decode('utf8'))
                found = True
                break
        if found == True:
            break

if __name__ == '__main__':
    main()