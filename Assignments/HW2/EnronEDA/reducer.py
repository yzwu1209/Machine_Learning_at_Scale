#!/usr/bin/env python
"""
Reducer takes words with their class and partial counts and computes totals.
INPUT:
    word \t class \t partialCount 
OUTPUT:
    word \t class \t totalCount  
"""
import re
import sys

# initialize trackers
current_word = None
spam_count, ham_count = 0,0

# read from standard input
for line in sys.stdin:
    # parse input
    word, is_spam, count = line.split('\t')
    
############ YOUR CODE HERE #########
    if current_word is None or current_word == word:
        if int(is_spam):
            spam_count += int(count)
        elif not int(is_spam):
            ham_count += int(count)
            
    elif current_word is not None and current_word != word:
        print("{}\t{}\t{}".format(current_word, 1, spam_count))
        print("{}\t{}\t{}".format(current_word, 0, ham_count))
        # reset spam and ham count to be zero
        spam_count, ham_count = 0, 0
        if int(is_spam):
            spam_count += int(count)
        elif not int(is_spam):
            ham_count += int(count)
    # Reset current_word to be word
    current_word = word
       
# Print out the spam and ham counts for the last word        
print("{}\t{}\t{}".format(current_word, 1, spam_count))
print("{}\t{}\t{}".format(current_word, 0, ham_count))
     

############ (END) YOUR CODE #########