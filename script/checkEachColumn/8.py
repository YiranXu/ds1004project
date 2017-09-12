from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader



if __name__ == "__main__":
   
    sc = SparkContext()
    lines_crime = sc.textFile(sys.argv[1]).map(lambda x: x.split('/t'))

    OFNS_DESC = lines_crime.map(lambda line: line[7])
    OFNS_DESC = OFNS_DESC.map(lambda x: [x, 'TEXT', 'description of offense', 'VALID'] if len(x)>0 else [x, 'TEXT', 'description of offense', 'NULL'])

    output = OFNS_DESC.map(lambda x: ' '.join(x))

    output.saveAsTextFile("c8.out")

    sc.stop()
