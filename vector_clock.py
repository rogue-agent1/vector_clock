#!/usr/bin/env python3
"""Vector clock — causality tracking for distributed systems.

One file. Zero deps. Does one thing well.

Tracks happened-before relationships between events across nodes.
Used in Dynamo, Riak, distributed debugging.
"""
import sys, copy

class VectorClock:
    def __init__(self, node_id=None):
        self.clock = {}
        self.node_id = node_id

    def increment(self, node_id=None):
        nid = node_id or self.node_id
        self.clock[nid] = self.clock.get(nid, 0) + 1
        return self

    def merge(self, other):
        result = VectorClock(self.node_id)
        all_keys = set(self.clock) | set(other.clock)
        for k in all_keys:
            result.clock[k] = max(self.clock.get(k, 0), other.clock.get(k, 0))
        return result

    def __le__(self, other):
        """self happened-before or equal to other."""
        return all(self.clock.get(k, 0) <= other.clock.get(k, 0) for k in self.clock)

    def __lt__(self, other):
        return self <= other and self.clock != other.clock

    def __eq__(self, other):
        return self.clock == other.clock

    def concurrent(self, other):
        return not (self <= other) and not (other <= self)

    def __repr__(self):
        items = ", ".join(f"{k}:{v}" for k, v in sorted(self.clock.items()))
        return f"VC({items})"

    def copy(self):
        vc = VectorClock(self.node_id)
        vc.clock = dict(self.clock)
        return vc


class VectorClockStore:
    """Key-value store with vector clock versioning (Dynamo-style)."""
    def __init__(self):
        self.data = {}  # key -> [(value, VectorClock)]

    def put(self, key, value, context=None):
        vc = context.copy() if context else VectorClock()
        vc.increment(vc.node_id or "client")
        if key not in self.data:
            self.data[key] = [(value, vc)]
            return vc
        # Remove versions dominated by new write
        surviving = []
        for v, c in self.data[key]:
            if not (c <= vc):
                surviving.append((v, c))
        surviving.append((value, vc))
        self.data[key] = surviving
        return vc

    def get(self, key):
        return self.data.get(key, [])


def main():
    print("Vector Clocks\n")
    a = VectorClock("A").increment()
    b = VectorClock("B").increment()
    print(f"a = {a}")
    print(f"b = {b}")
    print(f"a < b: {a < b}")
    print(f"concurrent: {a.concurrent(b)}")

    # Causal chain
    a2 = a.merge(b).copy()
    a2.node_id = "A"
    a2.increment()
    print(f"\na merges b then increments: {a2}")
    print(f"b < a2: {b < a2}")  # b happened before a2

    # Dynamo-style store
    print("\n=== Dynamo-style Store ===")
    store = VectorClockStore()
    # Node A writes
    ctx_a = VectorClock("A")
    ctx_a = store.put("cart", ["milk"], ctx_a)
    print(f"A writes ['milk']: {ctx_a}")
    # Node B reads old version and writes concurrently
    ctx_b = VectorClock("B")
    ctx_b = store.put("cart", ["eggs"], ctx_b)
    print(f"B writes ['eggs']: {ctx_b}")
    # Conflict! Two concurrent versions
    versions = store.get("cart")
    print(f"\nConflict detected: {len(versions)} versions")
    for val, vc in versions:
        print(f"  {val} @ {vc}")
    # Resolve by merging
    merged_vc = versions[0][1].merge(versions[1][1])
    merged_vc.node_id = "A"
    resolved = store.put("cart", ["milk", "eggs"], merged_vc)
    print(f"\nResolved: {store.get('cart')}")

if __name__ == "__main__":
    main()
