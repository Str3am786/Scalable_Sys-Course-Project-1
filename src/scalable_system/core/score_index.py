import heapq

class ScoreIndex:
    """
    Tracks (score, bike_id) so we can quickly drop the worst states.
    """
    def __init__(self):
        self._h = []          # heap of (score, bike_id)
        self._cur = {}        # bike_id -> score

    def upsert(self, bike_id: str, score: float) -> None:
        self._cur[bike_id] = score
        heapq.heappush(self._h, (score, bike_id))

    def drop_below(self, threshold: float) -> list[str]:
        removed = []
        while self._h and self._h[0][0] < threshold:
            score, bid = heapq.heappop(self._h)
            if self._cur.get(bid) == score:
                removed.append(bid)
                self._cur.pop(bid, None)
        return removed

    def drop_k_worst(self, k: int) -> list[str]:
        removed = []
        for _ in range(k):
            while self._h:
                score, bid = heapq.heappop(self._h)
                if self._cur.get(bid) == score:
                    removed.append(bid)
                    self._cur.pop(bid, None)
                    break
            else:
                break
        return removed

    def forget(self, bike_id: str) -> None:
        self._cur.pop(bike_id, None)

    def peek_min(self):
        """Return (score, bike_id) for the current minimum, or None."""
        import heapq
        while self._h:
            score, bid = self._h[0]
            if self._cur.get(bid) == score:
                return (score, bid)
            heapq.heappop(self._h)  # discard stale
        return None
