from pyspark.sql import SparkSession
from pyspark.sql import Row
from csv import reader

# Create pysparkSQL dataframe
spark = SparkSession \
    .builder \
    .appName("basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def mapper(fields):
    
    return Row(CMPLNT_NUM=str(fields[0].encode("utf-8")), CMPLNT_FR_DT=str(fields[1].encode("utf-8")), CMPLNT_FR_TM=str(fields[2].encode("utf-8")), CMPLNT_TO_DT=str(fields[3].encode("utf-8")), CMPLNT_TO_TM=str(fields[4].encode("utf-8")), RPT_DT=str(fields[5].encode("utf-8")), KY_CD=str(fields[6].encode("utf-8")), OFNS_DESC=str(fields[7].encode("utf-8")), PD_CD=str(fields[8].encode("utf-8")), PD_DESC=str(fields[9].encode("utf-8")), CRM_ATPT_CPTD_CD=str(fields[10].encode("utf-8")), LAW_CAT_CD=str(fields[11].encode("utf-8")), JURIS_DESC=str(fields[12].encode("utf-8")), BORO_NM=str(fields[13].encode("utf-8")), ADDR_PCT_CD=str(fields[14].encode("utf-8")), LOC_OF_OCCUR_DESC=str(fields[15].encode("utf-8")), PREM_TYP_DESC=str(fields[16].encode("utf-8")), PARKS_NM=str(fields[17].encode("utf-8")), HADEVELOPT=str(fields[18].encode("utf-8")), X_COORD_CD=str(fields[19].encode("utf-8")), Y_COORD_CD=str(fields[20].encode("utf-8")), Latitude=str(fields[21].encode("utf-8")), Longitude=str(fields[22].encode("utf-8")), Lat_Lon=str(fields[23].encode("utf-8")))

lines = spark.sparkContext.textFile("crime.csv").mapPartitions(lambda x: reader(x))
df = lines.map(mapper)


Df = spark.createDataFrame(df).cache()
Df.createOrReplaceTempView("tablename")


# read saved dataframe and extract information
df =sqlContext.read.parquet("dataframe") 
a = df.select('KY_CD').distinct()
b = a.orderBy(a.OFNS_DESC.desc())
a.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("file.csv")