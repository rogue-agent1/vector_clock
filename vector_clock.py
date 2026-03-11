#!/usr/bin/env python3
"""Vector clock implementation."""
import sys
class VectorClock:
    def __init__(self,pid,n): self.pid=pid; self.clock=[0]*n
    def tick(self): self.clock[self.pid]+=1; return self.clock[:]
    def send(self): self.tick(); return self.clock[:]
    def receive(self,other):
        for i in range(len(self.clock)): self.clock[i]=max(self.clock[i],other[i])
        self.tick()
    def happens_before(self,other):
        return all(a<=b for a,b in zip(self.clock,other)) and self.clock!=other
    def concurrent(self,other):
        return not self.happens_before(other) and not all(a<=b for a,b in zip(other,self.clock))
n=3; clocks=[VectorClock(i,n) for i in range(n)]
print("Vector Clock Demo (3 processes):")
ts1=clocks[0].send(); print(f"  P0 send: {ts1}")
ts2=clocks[1].send(); print(f"  P1 send: {ts2}")
clocks[2].receive(ts1); print(f"  P2 recv from P0: {clocks[2].clock}")
clocks[1].receive(clocks[2].send()); print(f"  P1 recv from P2: {clocks[1].clock}")
print(f"\n  P0{ts1} happens-before P2{clocks[2].clock}? {VectorClock(0,n).happens_before.__func__(type('',(),{'clock':ts1})(),clocks[2].clock)}")
