from pyspark.sql import SparkSession

def test_spark_dataframe_creation():
    spark = SparkSession.builder.master("local[*]").appName("TestJob").getOrCreate()

    data = [("Nagendra", 28), ("Aryan", 25)]
    df = spark.createDataFrame(data, ["name", "age"])

    assert df.count() == 2
    assert df.columns == ["name", "age"]

    spark.stop()