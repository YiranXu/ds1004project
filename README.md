# ds1004project
This is the repo for DS-GA 1004 Big Data course project, where we used Spark, Hive, MapReduce, and other HDFS-based techniques to perform data cleaning, data expoleration, and machine learning algorithms on large-scale dataset.

## how to run our code
1. Step1

Run data_cleaning.py to modify some typos in column 8, which generates an output file: crime_cleaned.out in HDFS. (XXX.csv is the name of input csv file)
```bash
spark-submit data_cleaning.py XXX.csv
```
2. Step2

Run scripts from 1.py to 16.py, which output X.out
```bash
spark-submit X.py crime_cleaned.out
```

3. Step3

Run scripts from 17.py to 24.py, which output X.out. (XXX.csv is the same as the one at step1.) 
```bash
spark-submit X.py XXX.csv
```
4. Step4 

Run checkTimeOrder.py and checkReportOrder.py, whech check time order issues in columns from 2 to 5.
```bash
spark-submit checkTimeOrder.py crime_cleaned.out
spark-submit checkReportOrder.py crime_cleaned.out
```
