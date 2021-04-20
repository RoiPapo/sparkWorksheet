import findspark

findspark.init()
from pyspark.sql import SparkSession


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def pprint_row(row, k=2):
    row_format = "{:^12}" * (k)
    print(row_format.format(*row[:k]))


def main():
    spark, sc = init_spark('Roi')
    data_file = 'globalterrorismdb.csv'
    terror_rdd = sc.textFile(data_file)
    # Split by comma:
    csv_rdd = terror_rdd.map(lambda row: row.split(','))
    # Split to header and data:
    header = csv_rdd.first()
    data_rdd = csv_rdd.filter(lambda row: row != header and 1983 <= int(row[0]) <= 2013)
    data_rdd = data_rdd.map(lambda row: (row[7], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda t: t[1], ascending=False) \
        .map(lambda t: (t[0], round(t[1] / 31, 1))) \
        .filter(lambda row: row[1] >= 10)
    pprint_row(["country", "AVG"])
    print('-' * 12 * 9)
    for row in data_rdd.collect():
        pprint_row(row)


if __name__ == "__main__":
    main()
