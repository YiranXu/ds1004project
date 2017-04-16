from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


if __name__ == "__main__":

	sc = SparkContext()
	lines_crime = sc.textFile('crime_cleaned.out').map(lambda x: x.split('/t'))

	FR_DT = lines_crime.map(lambda line: line[0])
	FR_DT = FR_DT.map(lambda x: [x, 'INT', 'persistent ID', 'NULL'] if len(x)==0 else ([x, 'INT', 'persistent ID', 'VALID'] if len(x)==9 else [x, 'INT', 'persistent ID', 'INVALID']))

	output = FR_DT.map(lambda x: ' '.join(x))

	output.saveAsTextFile("1.out")

	sc.stop()
