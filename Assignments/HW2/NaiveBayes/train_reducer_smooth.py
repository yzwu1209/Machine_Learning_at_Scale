#!/usr/bin/env python

import sys                                                  
import numpy as np                                          
                                                             
# set up Laplace Smoothing Parameters
V = 4555.0 # Enron training set vocab size
# V = 6.0  # China vocab size
k = 1.0 # set pseudocount as 1
pseudo = np.array([k,k])
vocalSize = np.array([V,V])

# initialize trackers [ham, spam]
docTotals = np.array([0.0, 0.0])
wordTotals = np.array([0.0, 0.0])
cur_word, cur_counts = None, np.array([0.0,0.0])

# read from standard input
for line in sys.stdin:
    wrd, counts = line.split('\t')
    counts = np.array([float(c) for c in counts.split(',')])

    # store totals, add or emit counts and reset 
    if wrd == "*docTotals": 
        docTotals += counts
    elif wrd == "*wordTotals": 
        wordTotals += counts 
    elif wrd == cur_word:
        cur_counts += counts
    else:
        if cur_word and cur_word != "*wordTotals":
            freq = (cur_counts + pseudo)/(wordTotals + vocalSize)
            num_to_print = ','.join(map(str, cur_counts)) + ',' + ','.join(map(str, freq))
            print(f"{cur_word}\t{num_to_print}")
        cur_counts  = np.array(counts)
    cur_word = wrd

# print out last record 
freq = (cur_counts + pseudo)/(wordTotals + vocalSize)
num_to_print = ','.join(map(str, cur_counts)) + ',' + ','.join(map(str, freq))
print(f"{cur_word}\t{num_to_print}")

# calculate class priors and print out
cur_word = 'ClassPriors'
prior_freq = docTotals/sum(docTotals)
num_to_print = ','.join(map(str, docTotals)) + ',' + ','.join(map(str, prior_freq))
print(f"{cur_word}\t{num_to_print}")

