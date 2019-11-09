######################## 1. Setup #########################
# Imports  
import ast
import time
import pyspark
import numpy as np
import pandas as pd

# Start pyspark 
sc = pyspark.SparkContext()

# Read in data to RDDs from text files that are saved on gcloud bucket
rdd = sc.textFile('gs://w261-bucket-yzwu/data/wiki_graph.txt')

##################### 2. Initialize graph #####################
# Define initGraph function - part 7c - job to initialize the graph 
def initGraph(dataRDD):
    """
    Spark job to read in the raw data and initialize an 
    adjacency list representation with a record for each
    node (including dangling nodes).
    
    Returns: 
        graphRDD -  a pair RDD of (node_id , (score, edges))
        
    NOTE: The score should be a float, but you may want to be 
    strategic about how format the edges... there are a few 
    options that can work. Make sure that whatever you choose
    is sufficient for Question 8 where you'll run PageRank.
    """
    ############## YOUR CODE HERE ###############    
    # write any helper functions here
    def parse(line):
        """
        Parsing each line of record:
        - create (key,value) pair
        - convert value format to dict
        """
        node, edges = line.split('\t')
        return (node, ast.literal_eval(edges))
    
    def getEdges(line):
        """
        For nodes that are the keys of RDD:
        - convert their format to a tuple of (node_id, edges)
        - This will only list out node that are existing RDD keys
        - If a node has duplicate edges, the function will emit duplicate items. 
        """
        edges = ''
        for key, value in line[1].items():
            edges += (key+',')*value
        return (line[0], edges[:-1])
    
    def convertEdgeToNode(line):
        """
        For nodes that are listed as edges of key nodes:
        - convert their format to a tuple of (node_id, [])
        """
        for node in line[1].split(','):
            yield (node, '')
    
    # write your main Spark code here
    # 1. Convert existing RDD to (node_id , list of edges)) format 
    keyRDD = dataRDD.map(parse) \
                    .map(getEdges) \
                    .cache()
    
    # 2. Create a new RDD with edges as keys
    edgeRDD = keyRDD.flatMap(convertEdgeToNode).cache()
    
    # 3. Merge both RDDs, then reduce by key
    init_graphRDD = keyRDD.union(edgeRDD) \
                          .reduceByKey(lambda x, y: (x + y)) \
                          .cache()
                        
    # 4. Compute N
    N = init_graphRDD.count()
    
    # 5. Update the init_graphRDD to include score as 1/N
    graphRDD = init_graphRDD.map(lambda x: (x[0], (1/N, x[1]))).cache()
    
    ############## (END) YOUR CODE ##############
    
    return graphRDD


##################### 3. Run PageRank  #####################
## 3.1 Provide FloatAccumulator class 
from pyspark.accumulators import AccumulatorParam

class FloatAccumulatorParam(AccumulatorParam):
    """
    Custom accumulator for use in page rank to keep track of various masses.
    
    IMPORTANT: accumulators should only be called inside actions to avoid duplication.
    We stringly recommend you use the 'foreach' action in your implementation below.
    """
    def zero(self, value):
        return value
    def addInPlace(self, val1, val2):
        return val1 + val2
    
## 3.2 Define runPageRank function 
def runPageRank(graphInitRDD, alpha = 0.15, maxIter = 10, verbose = True):
    """
    Spark job to implement page rank
    Args: 
        graphInitRDD  - pair RDD of (node_id , (score, edges))
        alpha         - (float) teleportation factor
        maxIter       - (int) stopping criteria (number of iterations)
        verbose       - (bool) option to print logging info after each iteration
    Returns:
        steadyStateRDD - pair RDD of (node_id, pageRank)
    """
    # teleportation:
    a = sc.broadcast(alpha)
    
    # damping factor:
    d = sc.broadcast(1-a.value)
    
    # initialize accumulators for dangling mass & total mass
    mmAccum = sc.accumulator(0.0, FloatAccumulatorParam())
    totAccum = sc.accumulator(0.0, FloatAccumulatorParam())
    
    ############## YOUR CODE HERE ###############
    
    # write your helper functions here, 
    # please document the purpose of each clearly 
    # for reference, the master solution has 5 helper functions.
    def mapper(line):
        """
        a mapper function to distribute node's score to linked edges:
        - linked edges would get the credits;
        - self will not get credits
        - credit is calculated by original score / (total of linked edges), duplciate edges will be counted 
        """
        node, score, edges = [line[0]], line[1][0], line[1][1]
        if edges == '':
            edge_list = []
            credit = 0
        else:
            edge_list = edges.split(',')
            nlinks = len(edge_list)
            credit = score / nlinks
        
        node_list = node + edge_list
        for i in node_list:
            if i == line[0]:
                yield (i, 0)
            else:
                yield (i, credit)
    
    def getDM(line, mmAccumulator, totAccumulator):
        """A function to update missing mass accumulator, and return input line"""
        if line[1][1] == '':
            mmAccumulator.add(line[1][0]) 
        totAccumulator.add(line[1][0])
    
    def updateScore(line, mm, G, a, d):
        """
        A function to update score:
        - mm: missing mass (or dangling mass), calculated by the value of missing mass accumulator
        - G: total number of distinct nodes
        - a: alpha, (float) teleportation factor
        - d: (1-alpha)
        """
        node, score = line[0], line[1]
        new_score = a/G + d*(mm/G + score)
        return (node, new_score)
    
        
    # write your main Spark Job here (including the for loop to iterate)
    # for reference, the master solution is 21 lines including comments & whitespace
    
    ## 1. Get the count of total distinct nodes
    G = graphInitRDD.count()
    # broadcast total nodes:
    G_bc = sc.broadcast(G)
    
    ## 2. Save edges of graph into a separate RDD
    edgeRDD = graphInitRDD.map(lambda x: (x[0], x[1][1])).cache()
    
    for i in range(maxIter):
        ## 3. Calculate the missing mass at the beginning of each iteration
        graphInitRDD.foreach(lambda x: getDM(x, mmAccum, totAccum))
        dangling_mass = sc.broadcast(mmAccum.value)
        total_mass = totAccum.value
        
        ## 4. Apply mapper function, reduce by key to combine credits from multiple sources, update score based off formula 
        graphInitRDD = graphInitRDD.flatMap(mapper) \
                                   .reduceByKey(lambda x, y: (x+y)) \
                                   .map(lambda x: updateScore(x, dangling_mass.value, G_bc.value, a.value, d.value)) \
                                   .rightOuterJoin(edgeRDD) \
                                   .cache()
        
        ## 5. If verbose=True, print out the missing mass and total mass for each iteration
        if verbose:
            print(f"Step {i}: missing mass = {dangling_mass.value}; total mass = {total_mass}")
        
        ## 6. Reset accumulators for dangling mass & total mass at the end of each iteration
        mmAccum = sc.accumulator(0.0, FloatAccumulatorParam())
        totAccum = sc.accumulator(0.0, FloatAccumulatorParam())
    
    ## 7. Output the steady state RDD after looping ends
    steadyStateRDD = graphInitRDD.map(lambda x: (x[0], x[1][0])).cache()
    ############## (END) YOUR CODE ###############
    
    return steadyStateRDD


nIter = 10
start = time.time()

# Initialize your graph structure (Q7)
wikiGraphRDD = initGraph(rdd)

# Run PageRank (Q8)
full_results = runPageRank(wikiGraphRDD, alpha = 0.15, maxIter = nIter, verbose = True)

print(f'...trained {nIter} iterations in {time.time() - start} seconds.')
print(f'Top 20 ranked nodes:')
print(full_results.takeOrdered(20, key=lambda x: -x[1]))

