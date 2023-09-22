from pyspark.sql import SparkSession
def spark_transform():
    from pyspark.sql.functions import col, concat_ws , explode ,when
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('covidReportAnalysis').getOrCreate()
    from functools import reduce
    df1 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data1.json")
    df2 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data2.json")
    df3 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data3.json")
    df4 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data4.json")
    df5 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data5.json")
    df6 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data6.json")
    df8 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data8.json")
    df9 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data9.json")
    df10 = spark.read.option("multiline","true").json("hdfs://localhost:9000/input/data10.json")

    data = [df1,df2,df3,df4,df5,df6,df8,df9,df10]
    k = 1;
    l = []
    for i in data:
        csv_df = i.select(col("paper_id"),col("metadata.title"),concat_ws(' ', col("abstract.text")).alias("abstract"))
        authors_df = i.select(col("paper_id"),explode(i.metadata.authors).alias("author"))
        authors_location = i.select(col("paper_id"),explode(i.metadata.authors.affiliation.location).alias("location"))
        authors_info_df = authors_df.select(
        col("paper_id"),
        col("author.first").alias("first"),
        col("author.last").alias("last"))
        authors_location_df = authors_location.select(col("paper_id"),col("location.country").alias("Place"))
        final_df = csv_df.join(authors_info_df, on="paper_id")
        final_ans_df = final_df.join(authors_location_df, on="paper_id")
        l.append(final_ans_df)

    analysis_df = reduce(lambda df1, df2: df1.union(df2), l)

    analysis_df.createOrReplaceTempView("analysis")

    result = spark.sql("select distinct paper_id , title ,abstract ,first, last, Place from analysis")
    result.createOrReplaceTempView("result_df")
    result.write.csv('hdfs://localhost:9000/output/result.csv', mode='overwrite', header=True)
    Number_of_paper = spark.sql("select count(distinct paper_id) as total_paper from result_df")
    Number_of_paper.write.csv('hdfs://localhost:9000/output/Number_of_paper.csv', mode='overwrite', header=True)
    paper_id_and_title = spark.sql("select distinct paper_id,title from result_df")
    paper_id_and_title.write.csv('hdfs://localhost:9000/output/paper_id_and_title.csv', mode='overwrite', header=True)
    paper_id_and_abstract = spark.sql("select distinct paper_id,abstract from result_df")
    paper_id_and_abstract.write.csv('hdfs://localhost:9000/output/paper_id_and_abstract.csv', mode='overwrite', header=True)

    auhtor_country = spark.sql("select distinct paper_id, first , last , Place from result_df")
    auhtor_country.write.csv('hdfs://localhost:9000/output/auhtor_country.csv', mode='overwrite', header=True)

spark_transform()
