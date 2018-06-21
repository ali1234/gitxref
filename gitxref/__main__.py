import argparse
import binascii
import pathlib
from collections import defaultdict
from contextlib import redirect_stdout

import numpy as np
from tqdm import tqdm

from gitxref.graph import Graph
from gitxref.repo import Repo
from gitxref.source import Source


def b2h(binsha):
    return binascii.hexlify(binsha).decode('utf8')


def group_commits(bitmaps):
    """groups commits which contain the same blobs"""
    commit_groups = defaultdict(list)
    for commit, array in tqdm(bitmaps.items(), unit=' commits', desc='Grouping commits'):
        commit_groups[array.tobytes()].append(commit)
    return list((v, np.frombuffer(k, dtype=np.uint8)) for k, v in commit_groups.items())


def find_best(source, commit_groups):
    unfound = np.empty(((len(source) + 7) // 8,), dtype=np.uint8)
    unfound[:] = 0xff
    unfound[-1] = ((0xff << ((8 - (len(source) % 8)) % 8)) & 0xff)

    keyfunc = lambda x: np.sum(np.unpackbits(x[1] & unfound))

    best = sorted(commit_groups, key=keyfunc, reverse=True)

    while len(best):
        inbest = best[0][1] & unfound
        yield (best[0][0], inbest)
        unfound &= ~best[0][1]
        best = sorted(filter(lambda x: keyfunc(x) > 0, best[1:]), key=keyfunc, reverse=True)
    yield ([], unfound)


def realmain(args):

    repo = Repo(args.repository, force_rebuild=args.rebuild, skip_cache=args.skip_cache, processes=args.processes)

    if args.directory is None:
        if args.rebuild:
            Graph(repo)
        return

    source = Source(args.directory)

    if args.debugging and 'commit_groups' in repo.cache:
        commit_groups = repo.cache['commit_groups']
    else:
        commit_groups = group_commits(Graph(repo).bitmaps(source))
        repo.cache['commit_groups'] = commit_groups

    for commits, bitmap in find_best(source, commit_groups):
        blob_indexes = np.flatnonzero(np.unpackbits(bitmap))
        print(', '.join(b2h(c)[:8] for c in commits), len(blob_indexes))
        for p in sorted(', '.join(str(p) for p in sorted(source[n].paths)) for n in blob_indexes):
            print('   ', p)


def main():
    parser = argparse.ArgumentParser(description='Git x ref.')
    parser.add_argument('repository', metavar='repository', type=pathlib.Path,
                        help='Path to Git repository.')
    parser.add_argument('directory', metavar='directory', type=pathlib.Path, default=None, nargs='?',
                        help='Path to unpacked tarball.')
    parser.add_argument('-r', '--rebuild', action='store_true',
                        help='Force rebuild of cached metadata.')
    parser.add_argument('-s', '--skip-cache', action='store_true',
                        help="Don't load or save the cached metadata (implies -r).")
    parser.add_argument('-d', '--debugging', action='store_true',
                        help="Use extra caching for debugging.")
    parser.add_argument('-p', '--processes', type=int, default=0,
                        help="Number of worker processes. Default: 0 (disable multiprocessing).")
    parser.add_argument('--profile', type=str, default=None,
                        help='Benchmark with cProfile.')

    args = parser.parse_args()
    if args.profile is not None:
        import cProfile
        p = cProfile.Profile()
        p.runcall(realmain, args)
        with open(args.profile, 'w') as f:
            with redirect_stdout(f):
                p.print_stats()

    else:
        realmain(args)

if __name__ == '__main__':
    main()
