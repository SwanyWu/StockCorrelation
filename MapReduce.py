"MapReduce.py"
import time

from correlationmeasure import computePC, computeMI
from pyspark.sql import SparkSession
import ast

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.driver.memory", "8g") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("dataframe.csv", header=True)

# Retrieve the symbols of all cryptocoins
coin_symbols = [row.symbol for row in df.select('symbol').distinct().collect()]

# Store all possible dates
year = 13
month = 5
dates = []
for i in range(0, 67):
    date = '20' + str(year) + '-' + str(month).zfill(2)
    dates.append(date)

    if month == 12:
        month = 1
        year += 1
    else:
        month += 1


# Return list of all possible pairs excluding reflexive and commutative pairs
def getValidPairs(list):
    result = []
    for i in range(0, len(list)):
        for j in range(i + 1, len(list)):
            result.append((list[i], list[j]))
    return result


# Store the possible coin pairs in pairDict to corresponding date
pairDict = {}
cols = ['open', 'close', 'high', 'low', 'volume', 'close_ratio', 'spread']
for date in dates:
    colDict = {}
    dateMatches = df.filter(df.date.contains(date))
    for col in cols:
        colMatches = dateMatches.select('symbol', col).collect()
        colList = [(row.symbol, getattr(row, col)) for row in colMatches]
        colDict[col] = getValidPairs(colList)
    pairDict[date] = colDict
    print(date)

print('Created pairDict')

# TODO: iterate through pairDict to get the list of pairs as input to MapReduce
dates_rdd = spark.sparkContext.parallelize(dates)
cols_rdd = spark.sparkContext.parallelize(cols)
print('Created pair_rdd')


def compute(tri):
    column = tri[0]
    symbol1 = tri[1][0]
    symbol2 = tri[1][1]
    p1 = df.filter(df.symbol == symbol1).filter(df.date == date).select(column).collect()
    p2 = df.filter(df.symbol == symbol2).filter(df.date == date).select(column).collect()
    p1Vec = [getattr(row, column) for row in p1]
    p2Vec = [getattr(row, column) for row in p2]
    p11 = ast.literal_eval(p1Vec[0])
    p22 = ast.literal_eval(p2Vec[0])
    PC = computePC(p11, p22)
    MI = computeMI(p11, p22)
    return [column, symbol1, symbol2, PC, MI]


def testFunc(pair):
    symbol1 = pair[0][0]
    symbol2 = pair[1][0]
    p1 = ast.literal_eval(pair[0][1])
    p2 = ast.literal_eval(pair[1][1])
    PC = computePC(p1, p2)
    MI = computeMI(p1, p2)
    return [symbol1, symbol2, PC, MI]


# Return key = 1 if threshold exceeded, else return key = 0
def threshold(input):
    # input[2] = PC, input[3] = MI
    if input[2] > 0.9:
        return 1
    return 0


for date in dates:
    for column in cols:
        start_time = time.time()
        pair_rdd = spark.sparkContext.parallelize(pairDict[date][column])
        mapping = pair_rdd.map(lambda x: testFunc(x))
        mapping = mapping.map(lambda x: (threshold(x), x)).groupByKey().mapValues(list)
        res = mapping.collect()

        with open('result.txt', 'a') as f:
            f.write(">Date =" + date + " | column = " + column + " | Finished MapReduce process in " + str(
                time.time() - start_time) + " s\n")
            # There are no pairs for which key = 1
            if len(res) != 2:
                f.write("NO PAIRS EXCEED THE THRESHOLD\n")
                continue
            for item in res[1][1]:
                f.write("%s\n" % item)