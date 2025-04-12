#!/usr/bin/env python3
import sys
from itertools import groupby
from operator import itemgetter

def read_mapper_output(file):
    for line in file:
        yield line.strip().split("\t", 1)

data = read_mapper_output(sys.stdin)

for key, group in groupby(data, key=itemgetter(0)):
    try:
        total = sum(int(count) for _, count in group)
        print(f"{key}\t{total}\n")
    except Exception as e:
        continue
