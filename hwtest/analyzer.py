#!/bin/env python
'''
Analyze the logs from a merger run.
Jan Veverka, veverka@mit.edu, 28 Nov 2014
'''

import os
fdir = '/nfshome0/veverka/daq/benchmark/logs/logs_v1.6'
fname = 'merger_optionC_run100_bu-c2d38-27-01_mini.out'

def main():
    data = []
    with file(os.path.join(fdir, fname)) as source:
        for line in source:
            record = parse(line)
            if record
                data.append(record)
    analyze(data)

def parse(line):
    return None

def analyze(data):
    print data[:10]

if __name__ == '__main__':
    main()

