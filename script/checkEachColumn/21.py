from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader

if __name__ =="__main__":
	if len(sys.argv)!=2:
		print("Usage: <file>",file=sys.stderr)
		exit(-1)
	sc=SparkContext()
	crime=sc.textFile(sys.argv[1]).mapPartitions(lambda x:reader(x))
	#we would exclude header when checking data-quality for each column
	def has_header(first_line):
		has_header=True
		if first_line[0]=='CMPLNT_NUM':
			has_header=True
		else:
			has_header=False
		return has_header
	header=crime.first()
	if has_header(header):
		crime=crime.filter(lambda row: row!=header)
	#column at index 20 (starting from 0) is Y_COORD_CD
	y_cord=crime.map(lambda row: row[20])
	lower_bound=110626.2880
	upper_bound=424498.0529
	def is_valid(cord):
		is_valid=True
		try: 
			cord=int(cord)
		except ValueError:
			is_valid=False
		if lower_bound<cord and cord<upper_bound:
			is_valid=True
		else:
			is_valid=False
		return is_valid
	y_cord=y_cord.map(lambda x:[x,'INT','Y-coordinate for NY State Plane Coordinate System Long Island zone','NULL'] if len(x)==0 else([x,'INT','Y-coordinate for NY State Plane Coordinate System Long Island zone','VALID'] if is_valid(x) else [x,'INT','Y-coordinate for NY State Plane Coordinate System Long Island zone','INVALID']))
	y_cord=y_cord.map(lambda x:' '.join(x))
	y_cord.saveAsTextFile("21.out")
	sc.stop()
