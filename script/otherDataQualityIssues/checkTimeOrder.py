from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


if __name__ == "__main__":


	def to_datetime(date_string, time=False):
		
		if time:
			date, t = date_string.split(' ')
			if t == '24:00:00':
				date_string = date + ' ' + '00:00:00'
			return datetime.datetime.strptime(date_string,"%m/%d/%Y %H:%M:%S") 
		else:
			return datetime.datetime.strptime(date_string,"%m/%d/%Y") 

	def exist(a,b,c='c',d='d'):
		return len(a) > 0 and len(b) > 0 and len(c)>0 and len(d)>0

	sc = SparkContext()
	lines_crime = sc.textFile('crime_cleaned').map(lambda x: x.split('/t'))

	FR_DT = lines_crime.map(lambda x: (to_datetime(x[1]+' '+x[2],True),to_datetime(x[3]+' '+x[4], True)) if exist(x[1],x[2],x[3],x[4]) else ((to_datetime(x[1]),to_datetime(x[3])) if exist(x[1],x[3]) else (1,1)))
	output = FR_DT.map(lambda x: 'valid order' if x[0]<x[1] else ('invalid order' if x[1]<x[0] else 'not all exist'))

	output.saveAsTextFile("checkTimeOrder.out")

	sc.stop()
