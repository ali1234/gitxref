import subprocess


class BatchCheck(object):
    """
    Implements the following shell pipeline to get object OIDs:

        git cat-file --buffer --batch-check='%(objecttype) %(objectname)' --batch-all-objects | grep -E \(^t\|^c\) | cut -d \  -f 2
    """

    def __init__(self, repo, types=None):
        self._repo = repo
        self._datapipe = None
        self._procs = []

        self._pipeline = [
            self.git_base + ['--batch-check=%(objecttype) %(objectname)', '--batch-all-objects'],
        ]

        if types is not None:
            pattern = '(^{:s})'.format('|^'.join(types))
            self._pipeline.append(['grep', '-E', pattern])

        self._pipeline.append(['cut', '-d', ' ', '-f', '2'])

    @property
    def git_base(self):
        return ['git', '-C', str(self._repo.git_dir), 'cat-file', '--buffer']

    def __enter__(self):
        for p in self._pipeline:
            proc = subprocess.Popen(p, stdin=self._datapipe, stdout=subprocess.PIPE)
            self._datapipe = proc.stdout
            self._procs.append(proc)

        return self

    def __exit__(self, *args):
        for p in self._procs:
            p.terminate()
        for p in self._procs:
            p.wait(timeout=2)
        for p in self._procs:
            p.kill()
        for p in self._procs:
            p.wait()

    def __iter__(self):
        for line in self._datapipe.readlines():
            yield line.strip()
