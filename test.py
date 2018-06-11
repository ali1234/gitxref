import argparse
import pathlib
from collections import defaultdict

import git
from gitdb.util import hex_to_bin

import timeit
import subprocess

def count_rev_list(repo):
    print('Counting with rev-list - this will NOT count dangling objects.')
    typecount = defaultdict(int)
    for line in repo.git.rev_list('--objects', '--all').split('\n'):
        binsha = hex_to_bin(line.split()[0])
        oinfo = repo.odb.info(binsha)
        typecount[oinfo.type] += 1
    print(', '.join('{:s}s: {:d}'.format(k.decode('utf8').capitalize(), v) for k, v in sorted(typecount.items())), 'Total:', sum(typecount.values()))


def count_cat_file(repo):
    print('Counting with cat-file - this WILL count dangling objects.')
    typecount = defaultdict(int)
    for line in repo.git.cat_file('--buffer', '--batch-all-objects', batch_check='%(objectname) %(objecttype)').split('\n'):
        type = line.strip().split(' ')[1]
        typecount[type] += 1
    print(', '.join('{:s}s: {:d}'.format(k.capitalize(), v) for k, v in sorted(typecount.items())), 'Total:', sum(typecount.values()))


def count_cat_file_direct(path):
    print('Counting with cat-file DIRECT - this WILL count dangling objects.')
    typecount = defaultdict(int)
    proc = subprocess.Popen(['git', '-C', str(path), 'cat-file', '--buffer', '--batch-all-objects', '--batch-check=%(objectname) %(objecttype)'], stdout=subprocess.PIPE)
    for line in proc.stdout:
        type = line.strip().split()[1]
        typecount[type] += 1
    print(', '.join('{:s}s: {:d}'.format(k.decode('utf8').capitalize(), v) for k, v in sorted(typecount.items())), 'Total:', sum(typecount.values()))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Git x ref.')
    parser.add_argument('repository', metavar='repository', type=pathlib.Path,
                        help='Path to Git repository.')

    args = parser.parse_args()

    repo = git.Repo(str(args.repository), odbt=git.GitCmdObjectDB)

    print(timeit.timeit('count_rev_list(repo)', setup='from __main__ import count_rev_list, repo', number=1), 'seconds.')
    print(timeit.timeit('count_cat_file(repo)', setup='from __main__ import count_cat_file, repo', number=1), 'seconds.')
    print(timeit.timeit('count_cat_file_direct(args.repository)', setup='from __main__ import count_cat_file_direct, args', number=1), 'seconds.')
