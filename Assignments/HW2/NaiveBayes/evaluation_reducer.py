#!/usr/bin/env python
"""
Reducer to calculate precision and recall as part
of the inference phase of Naive Bayes.
INPUT:
    ID \t true_class \t P(ham|doc) \t P(spam|doc) \t predicted_class
OUTPUT:
    precision \t ##
    recall \t ##
    accuracy \t ##
    F-score \t ##
         
Instructions:
    Complete the missing code to compute these^ four
    evaluation measures for our classification task.
    
    Note: if you have no True Positives you will not 
    be able to compute the F1 score (and maybe not 
    precision/recall). Your code should handle this 
    case appropriately feel free to interpret the 
    "output format" above as a rough suggestion. It
    may be helpful to also print the counts for true
    positives, false positives, etc.
"""
import sys

# initialize counters
FP = 0.0 # false positives
FN = 0.0 # false negatives
TP = 0.0 # true positives
TN = 0.0 # true negatives
CP = 0.0 # condition positive
PCP = 0.0 # predicted condition positive
total = 0.0 # total population

# read from STDIN
for line in sys.stdin:
    # parse input
    docID, class_, pHam, pSpam, pred = line.split()
    # emit classification results first
    print(line[:-2], class_ == pred)
    
    # then compute evaluation stats
#################### YOUR CODE HERE ###################
    if class_ == pred and class_ == '1':
        TP += 1.0
        CP += 1.0
        PCP += 1.0
    elif class_ == pred and class_ == '0':
        TN += 1.0
    elif class_ != pred and class_ == '1':
        FN += 1.0
        CP += 1.0
    elif class_ != pred and class_ == '0':
        FP += 1.0
        PCP += 1.0
    total += 1.0
        
precision = TP / PCP
recall = TP / CP
accuracy = (TP + TN)/total

if TP != 0: 
    Fscore = 2.0 * precision * recall / (precision + recall)
elif TP == 0:
    Fscore = 0.0

print('Total # Documents: \t{}'.format(total))
print('True Positives: \t{}'.format(TP))
print('True Negatives: \t{}'.format(TN))
print('False Positives: \t{}'.format(FP))
print('False Negatives: \t{}'.format(FN))
print('Accuracy: \t{}'.format(accuracy))
print('Precision: \t{}'.format(precision))
print('Recall: \t{}'.format(recall))
print('F-Score: \t{}'.format(Fscore))


#################### (END) YOUR CODE ###################
    