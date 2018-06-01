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

from gitxref.backrefs import Backrefs
from gitxref.source import Source
from gitxref.util import b2h




def main():

    parser = argparse.ArgumentParser(description='Git x ref.')
    parser.add_argument('directory', metavar='directory', type=pathlib.Path,
                        help='Path to unpacked tarball.')
    parser.add_argument('repository', metavar='repository', type=pathlib.Path,
                        help='Path to Git repository.')

    args = parser.parse_args()

    repo = git.Repo(str(args.repository))
    backrefs = Backrefs(repo)
    backrefs.optimize()

    source = Source.scan(args.directory, backrefs)
    while source.find_backrefs():
        print('Total matching commits:', len(source.commits))
        print('Best commit:', b2h(source.commits[0][1]), 'matches', source.commits[0][0], 'blobs.')
        print('Roots:')
        for c in backrefs.root_commits(source.commits[0][1]):
            print(' ', b2h(c))


if __name__ == '__main__':
    main()