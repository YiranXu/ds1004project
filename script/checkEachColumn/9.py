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
                      .flatMap(lambda x:[(x[8],1)]) \
                      .map(lambda (k,v): k+ '\t' +'INT' +'\t'+ 'internal classification code' +'\t'+'VALID' if (k>='100' and k<='999') else k+ '\t' +'INT'+'\t'+ 'internal classification code' +'\t'+'NULL')
    counts.saveAsTextFile("9.out")
    sc.stop()
