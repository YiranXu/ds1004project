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
	latlon=crime.map(lambda row: row[23])
	latlon=latlon.map(lambda x:[x,'SET','Latitude and Longitude tuple','VALID'] if len(x)>0 else [x,'SET','Latitude and Longitude tuple','NULL'])
	latlon=latlon.map(lambda x:' '.join(x))
	latlon.saveAsTextFile("Lat_Lon.out")
	sc.stop()
