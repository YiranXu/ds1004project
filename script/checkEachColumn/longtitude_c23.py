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
	#column at index 22 (starting from 0) is Longtitude
	long=crime.map(lambda row: row[22])
	lower_bound=-74.270
	upper_bound=-71.750
	def is_valid(longtitude):
		is_valid=True
		try: 
			longtitude=float(longtitude)
		except ValueError:
			is_valid=False
		if lower_bound<longtitude and longtitude<upper_bound:
			is_valid=True
		else:
			is_valid=False
		return is_valid
	long=long.map(lambda x:[x,'DECIMAL','Longtitude with WGS 1984 standard','NULL'] if len(x)==0 else([x,'DECIMAL','Longtitude with WGS 1984 standard','VALID'] if is_valid(x) else [x,'DECIMAL','Longtitude with WGS 1984 standard','INVALID']))
	long=long.map(lambda x:' '.join(x))
	long.saveAsTextFile("Longtitude.out")
	sc.stop()
