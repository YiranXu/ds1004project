from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader



if __name__ == "__main__":
	'''
	We notice that there are some typos for VALID records, like 'OTHER STATE LAWS (NON PENAL LAW)' and 
																'OTHER STATE LAWS (NON PENAL LA'.
	So we will modify those typos to achieve consistency within VALID values before checking NULL and INVALID values. 

	Input:
		original csv file.
	Output:
		'/t' seperated string for each line 
	'''

	sc = SparkContext()
	crimes = sc.textFile('crime.csv').mapPartitions(lambda x: reader(x))

	crimes = crimes.map(lambda line: line[:7]+['OTHER STATE LAWS (NON PENAL LAW)']+line[8:] if line[7]=='OTHER STATE LAWS (NON PENAL LA' else line)
	crimes = crimes.map(lambda line: line[:7]+['KIDNAPPING & RELATED OFFENSES']+line[8:] if line[7]=='KIDNAPPING AND RELATED OFFENSES' else line)

	output = crimes.map(lambda x: '/t'.join(x))
	output.saveAsTextFile("crime_cleaned.out")   

	sc.stop()