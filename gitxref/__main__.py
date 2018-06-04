import argparse
import pathlib

import git

from gitxref.backrefs import Backrefs
from gitxref.source import Source
from gitxref.util import b2h


def main():

    parser = argparse.ArgumentParser(description='Git x ref.')
    parser.add_argument('repository', metavar='repository', type=pathlib.Path,
                        help='Path to Git repository.')
    parser.add_argument('directory', metavar='directory', type=pathlib.Path, default=None,
                        help='Path to unpacked tarball.')
    parser.add_argument('-R', '--rebuild', action='store_true',
                        help='Rebuild the backrefs cache (slow).')

    args = parser.parse_args()

    repo = git.Repo(str(args.repository))
    backrefs = Backrefs(repo, rebuild=args.rebuild)

    if args.directory is None:
        return

    source = Source(args.directory, backrefs)
    source.find_backrefs()
    for best, bits in source.find_best():
        print('Unfound:' if best is None else b2h(best), sum(bits))
        for binsha, path in source[bits]:
            print('    ', path)


if __name__ == '__main__':
    main()