
class Dedup(object):

    """
    Ensures that only one copy of an object exists.

    if a == b and a is not b:
        seen = Dedup()
        a = seen[a]
        b = seen[b]
        assert(a == b)
        assert(a is b)
    """

    def __init__(self):
        self._dict = {}
        self._eliminated = 0

    def __getitem__(self, key):
        if key in self._dict:
            self._eliminated += 1
            return self._dict[key]
        else:
            self._dict[key] = key
            return key

    def __len__(self):
        return len(self._dict)

    @property
    def eliminated(self):
        return self._eliminated