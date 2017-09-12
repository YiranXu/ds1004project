from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


if __name__ == "__main__":

	def is_valid(d):
		if d == '24:00:00':
			d = '00:00:00'
		try:
			datetime.datetime.strptime(d, '%H:%M:%S')
			return True
		except ValueError:
			return False

	sc = SparkContext()
	lines_crime = sc.textFile(sys.argv[1]).map(lambda x: x.split('/t'))

	FR_DT = lines_crime.map(lambda line: line[2])
	FR_DT = FR_DT.map(lambda x: [x, 'DATETIME', 'time of occurrence', 'NULL'] if len(x)==0 else ([x, 'DATETIME', 'time of occurrence', 'VALID'] if is_valid(x) else [x, 'DATETIME', 'time of occurrence', 'INVALID']))

	output = FR_DT.map(lambda x: ' '.join(x))

	output.saveAsTextFile("3.out")

	sc.stop()
