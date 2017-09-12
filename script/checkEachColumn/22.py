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
	#column at index 21 (starting from 0) is Latitude
	lat=crime.map(lambda row: row[21])
	lower_bound=40.470
	upper_bound=41.310
	def is_valid(latitude):
		is_valid=True
		try: 
			latitude=float(latitude)
		except ValueError:
			is_valid=False
		if lower_bound<latitude and latitude<upper_bound:
			is_valid=True
		else:
			is_valid=False
		return is_valid
	lat=lat.map(lambda x:[x,'DECIMAL','Latitude with WGS 1984 standard','NULL'] if len(x)==0 else([x,'DECIMAL','Latitude with WGS 1984 standard','VALID'] if is_valid(x) else [x,'DECIMAL','Latitude with WGS 1984 standard','INVALID']))
	lat=lat.map(lambda x:' '.join(x))
	lat.saveAsTextFile("22.out")
	sc.stop()
