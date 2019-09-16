#!/usr/bin/env python
"""
Reducer aggregates word counts by class and emits frequencies.
INPUT:
    word \t class0_partialCount,class1_partialCount
OUTPUT:
    word \t ham_count,spam_count,P(ham|word),P(spam|word)
    
Instructions:
    Again, you are free to design a solution however you see 
    fit as long as your final model meets our required format
    for the inference job we designed in Question 8. Please
    comment your code clearly and concisely.
    
    A few reminders: 
    1) Don't forget to emit Class Priors (with the right key).
    2) In python2: 3/4 = 0 and 3/float(4) = 0.75
"""
##################### YOUR CODE HERE ####################
# import packages                                                   
import sys                                                  
import numpy as np  

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
            freq = cur_counts/wordTotals
            num_to_print = ','.join(map(str, cur_counts)) + ',' + ','.join(map(str, freq))
            print(f"{cur_word}\t{num_to_print}")
        cur_counts  = np.array(counts)
    cur_word = wrd

# print out last record 
freq = cur_counts / wordTotals 
num_to_print = ','.join(map(str, cur_counts)) + ',' + ','.join(map(str, freq))
print(f"{cur_word}\t{num_to_print}")

# calculate class priors and print out
cur_word = 'ClassPriors'
prior_freq = docTotals/sum(docTotals)
num_to_print = ','.join(map(str, docTotals)) + ',' + ','.join(map(str, prior_freq))
print(f"{cur_word}\t{num_to_print}")

##################### (END) CODE HERE ####################