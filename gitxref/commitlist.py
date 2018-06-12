import subprocess


class CommitList(object):

    def __init__(self, repo, limit=None):
        self._limit = limit
        self._repo = repo

    def __enter__(self):
        git_base = ['git', '-C', str(self._repo.git_dir), 'cat-file', '--buffer']

        pipeline = [
            git_base + ['--batch-check=%(objecttype) %(objectname)', '--batch-all-objects'],
            ['grep', '-E', '(^c)'],
            ['cut', '-d', ' ', '-f', '2'],
        ]

        if self._limit is not None:
            pipeline.append(['head', '-n', str(self._limit)],)

        self._datapipe = None
        self._procs = []

        for p in pipeline:
            proc = subprocess.Popen(p, stdin=self._datapipe, stdout=subprocess.PIPE)
            self._datapipe = proc.stdout
            self._procs.append(proc)

        return self

    def __exit__(self, *args):
        for p in self._procs:
            p.terminate()
        for p in self._procs:
            p.wait(timeout=3)
        for p in self._procs:
            p.kill()
        for p in self._procs:
            p.wait()

    def __iter__(self):
        for line in self._datapipe.readlines():
            yield line.strip()

    def __len__(self):
        if self._limit is None:
            return 900000
        else:
            return self._limit