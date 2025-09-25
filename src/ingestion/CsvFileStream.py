import csv
from opencep.stream.Stream import InputStream


class CSVInputStream(InputStream):

    def __init__(self, file_path: str, row_to_event:callable, limit: int|None = None):
        super().__init__()
        n = 0
        with open(file_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ev = row_to_event(row)
                if ev is None:
                    continue
                self._stream.put(ev)
                n+=1
                if limit and n >= limit:
                    break
        self.close()




class BurstyInputStream(InputStream):
    def __init__(self, events_iterable, schedule_fn):
        super().__init__()
        # schedule_fn: i -> logical arrival bucket or sequence
        for i, ev in enumerate(events_iterable):
            ev["_arrival_seq"] = schedule_fn(i, ev)  # purely logical
            self._stream.put(ev)
        self.close()
