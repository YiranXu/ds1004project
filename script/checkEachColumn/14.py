from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])
    #lines = lines.mapPartitions(lambda x: reader(x))

    #Your code goes here
    counts = lines.map(lambda x: x.split('/t')) \
                      .flatMap(lambda x:[(x[13],1)]) \
                      .map(lambda (k,v): k+ '\t' + 'TEXT' + '\t' + 'Borough'+ '\t'+ 'VALID' if (len(k)>0) else k+ '\t' + 'TEXT' + '\t' + 'Borough'+ '\t'+ 'NULL')
    counts.saveAsTextFile("14.out")
    sc.stop()
