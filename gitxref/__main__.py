import argparse
import binascii
import pathlib

import numpy as np

from gitxref.graph import Graph
from gitxref.repo import Repo
from gitxref.source import Source


def main():
    parser = argparse.ArgumentParser(description='Git x ref.')
    parser.add_argument('repository', metavar='repository', type=pathlib.Path,
                        help='Path to Git repository.')
    parser.add_argument('directory', metavar='directory', type=pathlib.Path, default=None, nargs='?',
                        help='Path to unpacked tarball.')
    parser.add_argument('-r', '--rebuild', action='store_true',
                        help='Force rebuild of the graph cache.')
    parser.add_argument('-s', '--skip-cache', action='store_true',
                        help="Don't load or save the graph cache (implies -r).")
    #parser.add_argument('-p', '--processes', type=int, default=None,
    #                    help="Number of worker processes. '0' to disable multiprocessing.")

    args = parser.parse_args()
    repo = Repo(args.repository)
    graph = Graph(repo, skip_cache=args.skip_cache, rebuild=args.rebuild)

    if args.directory is None:
        return

    source = Source(repo, args.directory)
    source.make_bitmaps(graph)

    for best, bits in source.find_best():
        print('Unfound:' if best is None else binascii.hexlify(best).decode('utf8'), np.sum(np.unpackbits((bits))))
        for path, binsha in sorted(source[bits]):
            print('    ', path)


if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()')
    main()
