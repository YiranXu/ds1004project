import sys
from pyspark import SparkContext
from csv import reader
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext

if __name__ =="__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	data=sc.textFile("data_for_time.csv").mapPartitions(lambda x:reader(x))
	dataDF=data.toDF(['date','type','num','quarter','year','month','weekday'])
	sqlContext.registerDataFrameAsTable(dataDF,'dataDF')
	result=sqlContext.sql("SELECT SUM(num),year FROM dataDF GROUP BY year")	
	result.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("crimesPerYear.csv")
	sc.stop()
