from collections import OrderedDict

class LRU:

    def __init__(self, cap: int):
        self.cap = cap
        self.od = OrderedDict()


    def touch(self, key: str):
        if key in self.od:
            self.od.move_to_end(key, last=True)
        else:
            self.od[key] = None
            if len(self.od) > self.cap:
                return self.od.popitem(last=False)[0]
        return None

    def remove(self, key: str):
        self.od.pop(key, None)