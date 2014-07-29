#!/bin/env python
# -*- coding: utf-8 -*-

#_______________________________________________________________________________
def main():
    lumi_length_mean = 5.0       # LUMI_LENGTH_MEAN in launch.sh
    number_of_producers = 9      # Number of uncommented lines in listProducers.txt
    number_of_files_per_ls = 11  # NumberOfFilesPerLS in makeFiles.py
    total_file_size_in_mb = 400. # for all streams
    number_of_ls = 50            # ls in run100.cfg

    bandwidth_per_ls_in_gb = (number_of_producers *
                              number_of_files_per_ls *
                              total_file_size_in_mb) / 1024.

    print "Max expected throughput: %.0f GB / %.0f s = %.2f GB/s" % (
        bandwidth_per_ls_in_gb * number_of_ls,
        lumi_length_mean * number_of_ls,
        bandwidth_per_ls_in_gb / lumi_length_mean
        )
    return None
# main


#_______________________________________________________________________________
if __name__ == '__main__':
    main()