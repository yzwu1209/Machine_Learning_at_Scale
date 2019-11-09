import pyspark
from subprocess import call
import os, os.path

sc = pyspark.SparkContext()
rdd = sc.parallelize(['Hello,', 'world!', 'dog', 'elephant', 'panther'])

words = sorted(rdd.collect())

folder = 'data/'
if not os.path.exists(folder):
    os.mkdir(folder)
    with open(os.path.join(folder,'test2.txt'),'w+') as file_handler:
        file_handler.write("{}\n".format(words))
        file_handler.close()
        
print(words)