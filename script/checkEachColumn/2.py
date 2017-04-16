from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


if __name__ == "__main__":

	upper = datetime.datetime.strptime("01/01/2017","%m/%d/%Y")
	lower = datetime.datetime.strptime("12/31/1950","%m/%d/%Y")
	def is_valid(date_string):
		return lower < datetime.datetime.strptime(date_string,"%m/%d/%Y") and upper > datetime.datetime.strptime(date_string,"%m/%d/%Y")

	sc = SparkContext()
	lines_crime = sc.textFile('crime_cleaned.out').map(lambda x: x.split('/t'))

	FR_DT = lines_crime.map(lambda line: line[1])
	FR_DT = FR_DT.map(lambda x: [x, 'DATETIME', 'date of occurrence', 'NULL'] if len(x)==0 else ([x, 'DATETIME', 'date of occurrence', 'VALID'] if is_valid(x) else [x, 'DATETIME', 'date of occurrence', 'INVALID']))

	output = FR_DT.map(lambda x: ' '.join(x))

	output.saveAsTextFile("2.out")

	sc.stop()
