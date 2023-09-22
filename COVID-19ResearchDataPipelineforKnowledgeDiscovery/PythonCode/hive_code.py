from pyspark.sql import SparkSession
def spark_hive_store():
    spark = SparkSession.builder \
    .appName("HiveToDatabaseTransfer") \
    .master("local[1]") \
    .config("spark.master", "local") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

    spark.sql("create database if not exists covid_data")

    spark.sql("create table if not exists covid_data.result \
    (paper_id STRING, title STRING, abstract STRING,first_name STRING,last_name STRING, Place STRING) \
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
    tblproperties(\"skip.header.line.count\"=\"1\")")
    spark.sql("create table if not exists covid_data.Number_of_paper \
    (total_paper int)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
    tblproperties(\"skip.header.line.count\"=\"1\")")
    spark.sql("create table if not exists covid_data.paper_id_and_title \
    (paper_id STRING, title STRING) \
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
    tblproperties(\"skip.header.line.count\"=\"1\")")
    spark.sql("create table if not exists covid_data.paper_id_and_abstract \
    (paper_id STRING, abstract STRING) \
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
    tblproperties(\"skip.header.line.count\"=\"1\")")



    spark.sql("LOAD DATA INPATH '/output/result.csv/*.csv' INTO TABLE covid_data.result")
    spark.sql("LOAD DATA INPATH '/output/Number_of_paper.csv/*.csv' INTO TABLE covid_data.Number_of_paper")
    spark.sql("LOAD DATA INPATH '/output/paper_id_and_title.csv/*.csv' INTO TABLE covid_data.paper_id_and_title")
    spark.sql("LOAD DATA INPATH '/output/paper_id_and_abstract.csv/*.csv' INTO TABLE covid_data.paper_id_and_abstract")
    spark.stop()
spark_hive_store()
