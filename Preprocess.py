"""Preprocess.py"""
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, StringType

from correlationmeasure import computeMI, computePC

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .master("local[*]") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.sql.broadcastTimeout", "36000") \
    .getOrCreate()

# Creates DataFrame from .csv file
df = spark.read.csv("crypto-markets.csv", header=True)

# Drop first columns
newDf = df.drop('slug')

# Retrieve the symbols of all cryptocoins
coin_symbols = [row.symbol for row in newDf.select('symbol').distinct().collect()]
# Remove symbols that cannot be used as file name in Windows
coin_symbols.remove('AUX')

# Store each coin its rows as a DataFrame and save it as a .csv file
DataFrameDict = {symbol: newDf for symbol in coin_symbols}
# for key in DataFrameDict.keys():
#     """Uncomment block to generate .csv files for each cryptocoin"""
#     # rowList = newDf.filter(newDf.symbol == key).collect()
#     # rdd = spark.sparkContext.parallelize(rowList)
#     # df = rdd.toDF()
#     # fileName = 'dataframes/' + key + '.csv'
#     # df.toPandas().to_csv(fileName, index=False)
#
#     fileName = 'dataframes/' + key + '.csv'
#     DataFrameDict[key] = spark.read.csv(fileName, header=True)
#
# print('DataFrameDict initialized')

# df1 = spark.read.csv('dataframes/BTC.csv', header=True).select('open').collect()
# df2 = spark.read.csv('dataframes/ETH.csv', header=True).select('open').collect()
# openList1 = [float(row.open) for row in df1][:1211]
# openList2 = [float(row.open) for row in df2]
# result = computePC(openList1, openList2)
# result2 = computeMI(openList1, openList2)
# print(result)
# print(result2)

# Store all possible months and their amount of days in DateDict
year = 13
month = 5
DateDict = {}


def getDate():
    date = '20' + str(year) + '-' + str(month).zfill(2)
    return date


def incMonth():
    global year, month
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    return


# From '2013-05' to '2018-11' store amount of days per month in DateDict
# Temporary modified to 2017-01 to 2018-11
for i in range(0, 67):
    curDate = getDate()
    dateColumn = newDf.select('date')
    distDates = dateColumn.filter(dateColumn.date.contains(curDate)).distinct().collect()
    dayCount = len(distDates)
    DateDict[curDate] = dayCount
    incMonth()
print('DateDict created')

# Create DataFrame as in section 4 in overleaf
schemaFields = [StructField("symbol", StringType(), True),
                StructField("name", StringType(), True),
                StructField("date", StringType(), True),
                StructField("open", ArrayType(DoubleType(), True)),
                StructField("high", ArrayType(DoubleType(), True)),
                StructField("low", ArrayType(DoubleType(), True)),
                StructField("close", ArrayType(DoubleType(), True)),
                StructField("volume", ArrayType(DoubleType(), True)),
                StructField("close_ratio", ArrayType(DoubleType(), True)),
                StructField("spread", ArrayType(DoubleType(), True))]
schema = StructType(schemaFields)
# mainDf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
counter = 0
for coin in coin_symbols:
    fileName = 'dataframes/' + coin + '.csv'
    df = spark.read.csv(fileName, header=True)
    coin_name = df.select('name').collect()[0][0]
    newRows = []

    for date in DateDict.keys():
        rows = df.filter(df.date.contains(date)).collect()
        # No missing data rows for this month
        if len(rows) == DateDict[date]:
            openVec = [float(row.open) for row in rows]
            highVec = [float(row.high) for row in rows]
            lowVec = [float(row.low) for row in rows]
            closeVec = [float(row.close) for row in rows]
            volumeVec = [float(row.volume) for row in rows]
            c_rVec = [float(row.close_ratio) for row in rows]
            spreadVec = [float(row.spread) for row in rows]
            newRow = Row(symbol=coin, name=coin_name, date=date, open=openVec,
                         high=highVec, low=lowVec, close=closeVec,
                         volume=volumeVec, close_ratio=c_rVec, spread=spreadVec)
            newRows.append(newRow)

    monthDf = spark.createDataFrame(newRows, schema)
    # mainDf = mainDf.union(monthDf)
    counter += 1
    print(counter)
    with open('dataframe.csv', 'a', newline='') as f:
        monthDf.toPandas().to_csv(f, index=False, header=False)
print('done')