import subprocess


class GitProc(object):
    def __init__(self, git_dir, args):
        self._git_dir = git_dir
        self._args = args

    def __enter__(self):
        self._proc = subprocess.Popen(['git', '-C', str(self._git_dir)] + self._args, stdout=subprocess.PIPE)
        return self

    def __exit__(self, *args):
        self._proc.terminate()
        self._proc.wait(timeout=5)
        self._proc.kill()
        self._proc.wait()

    def __iter__(self):
        return self._proc.stdout

    def read(self):
        return subprocess.check_output(['git', '-C', str(self._git_dir)] + self._args)