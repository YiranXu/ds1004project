from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader



if __name__ == "__main__":
   
    sc = SparkContext()
    lines_crime = sc.textFile('crime_cleaned').map(lambda x: x.split('/t'))

    KY_CD = lines_crime.map(lambda line: line[6])
    KY_CD = KY_CD.map(lambda x: [x, 'INT', 'offense classification code', 'VALID'] if len(x)>0 else [x, 'INT', 'offense classification code', 'NULL'])

    output = KY_CD.map(lambda x: ' '.join(x))

    output.saveAsTextFile("c7.out")

    sc.stop()
