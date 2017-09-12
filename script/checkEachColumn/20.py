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
	#column at index 19 (starting from 0) is X_COORD_CD
	x_cord=crime.map(lambda row: row[19])
	lower_bound=909126.0155
	upper_bound=1610215.3590
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
	x_cord=x_cord.map(lambda x:[x,'INT','X-coordinate for NY State Plane Coordinate System Long Island zone','NULL'] if len(x)==0 else([x,'INT','X-coordinate for NY State Plane Coordinate System Long Island zone','VALID'] if is_valid(x) else [x,'INT','X-coordinate for NY State Plane Coordinate System Long Island zone','INVALID']))
	x_cord=x_cord.map(lambda x:' '.join(x))
	x_cord.saveAsTextFile("20.out")
	sc.stop()
