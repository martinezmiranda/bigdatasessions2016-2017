# import and initialize Spark context
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('wordCount')
sc = SparkContext(conf=conf)

# use Spark to count words
txt = sc.textFile("elQuijoteSpark.txt")
words = txt.flatMap(lambda txt: txt.split(" "))
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# store results
counts.saveAsTextFile("out.dat")