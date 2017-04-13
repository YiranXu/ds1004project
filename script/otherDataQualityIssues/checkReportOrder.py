from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


if __name__ == "__main__":

	def to_datetime(date_string):
		return datetime.datetime.strptime(date_string,"%m/%d/%Y") 

	def exist(a,b):
		return len(a) > 0 and len(b) > 0

	sc = SparkContext()
	lines_crime = sc.textFile('crime_cleaned').map(lambda x: x.split('/t'))

	FR_DT = lines_crime.map(lambda x: (to_datetime(x[1]),to_datetime(x[5]))if exist(x[1],x[5]) else (-1,-1)) 

	output = FR_DT.map(lambda x: (x[0], x[1], 'not both exist') if x[0]== -1 else ((x[0], x[1],'valid order') if x[1]>=x[0] else (x[0], x[1],'invalid order')))

	output.saveAsTextFile("checkReportOrder.out")

	sc.stop()
