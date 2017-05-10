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
## Plots
For part1:
We used GroupBy method in pyspark.sql to get summaries of data.
We plotted those summaries using Tableau Software.


For part2:
We created a pyspark.sql dataframe, then mainly used GroupBy method to get summaries of the whole data set. Then we did the analysis and visualization using python and Tableau. For example, 

Run"groupbyYear.py" in pyspark.
Run code like **.ipynb in ipython notebook.

## Data
After getting summaries of the crime dataset using spark and downloading other datasets from other websites, we performed some analysis and data transformation. So we only put the final step data in this repo, and you can find data needed to generate plots. 
