#!/usr/bin/env python
"""
This script reads word counts from STDIN and combines
the counts for any duplicated words.

INPUT & OUTPUT FORMAT:
    word \t count
USAGE:
    python collateCounts.py < yourCountsFile.txt

Instructions:
    For Q6 - Use the provided code as is. (you'll need to uncomment it)
    For Q7 - Delete or comment out the section marked "PROVIDED CODE" &
             replace it with your own implementation. Your solution
             should not use a dictionary or store anythin other than a
             signle total count - just print them as soon as you've
             added them. HINT: you've modified the framework script
             to ensure that the input is alphabetized; how can you
             use that to your advantage?
"""

# imports
import sys
from collections import defaultdict

########### PROVIDED IMPLEMENTATION ##############
##### uncomment to run
# counts = defaultdict(int)
# # stream over lines from Standard Input
# for line in sys.stdin:
#     # extract words & counts
#     word, count  = line.split()
#     # tally counts
#     counts[word] += int(count)
# # print counts
# for wrd, count in counts.items():
#     print("{}\t{}".format(wrd,count))
########## (END) PROVIDED IMPLEMENTATION #########

################# YOUR CODE HERE #################
temp_total = 0
isfirst = True

for line in sys.stdin:
    # extract words & counts
    word, count  = line.split()

    # store the first word as temp_word, and its count as temp_total
    if isfirst:
        temp_word = word
        temp_total = int(count)
        isfirst = False
        continue

    # starting from second word, compare word with temp_word
    else:
        # if word and temp_word are the same, add its count to temp_total
        if temp_word == word:
            temp_total += int(count)
        # if word and temp_word are not the same, then temp_word reaches to the end
        # complete counting of temp_word
        # print out temp_word and its total count temp_total
        # reset temp_total to the count of current word
        else:
            print("{}\t{}".format(temp_word, temp_total))
            temp_word = word
            temp_total = int(count)












################ (END) YOUR CODE #################
