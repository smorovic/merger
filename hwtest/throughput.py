#!/usr/bin/env python
## USAGE: ./throughput.py 1500 06:29:20 07:09:31
import sys

_stream_A_fraction = 200./400.

#______________________________________________________________________________
def main():
    script, bandwidth, start, end = sys.argv
    bandwidth = float(bandwidth)
    time = time_in_seconds(end) - time_in_seconds(start)
    if time < 0:
        time += 24 * 3600
    throughput = bandwidth / time
    print 'Average throughput:',
    print '%.0f GB / %d s = %.2f GB/s' % (bandwidth, time, throughput),
    # print '(%.2f GB/s stream A,' % (_stream_A_fraction * throughput,),
    # print '%.2f GB/s others)' % ((1 - _stream_A_fraction) * throughput,)
## main


#______________________________________________________________________________
def time_in_seconds(time_string):
    hours, minutes, seconds = map(int, time_string.split(':'))
    return 3600 * hours + 60 * minutes + seconds
## time_in_seconds


#______________________________________________________________________________
if __name__ == '__main__':
    main()
