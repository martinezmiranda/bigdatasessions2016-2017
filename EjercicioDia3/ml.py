from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils
from numpy import array

conf = SparkConf().setAppName('mldemo')
sc = SparkContext(conf=conf)
spark = HiveContext(sc)


# Load and parse the data file into an RDD of LabeledPoint.
raw_training_data = spark.sql("select * from sparkml.trainingdata")
raw_test_data = spark.sql("select * from sparkml.testdata")

def parse_raw(line):
    clean_line_split = line[1:10]
    winner = line[0]
    return LabeledPoint(winner, array([float(x) for x in clean_line_split]))

training_data = raw_training_data.rdd.map(parse_raw)
test_data = raw_test_data.rdd.map(parse_raw)

# Train a DecisionTree model.
# With a bigger depth
model = DecisionTree.trainRegressor(training_data, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=20, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(test_data.map(lambda x: x.features))
labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
textWithPrediction = test_data.zip(predictions)
textWithPrediction.saveAsTextFile("output")
csv =raw_training_data.rdd.zip(predictions).map(toCSVLine)


testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(test_data.count())
print('Test Mean Squared Error = ' + str(testMSE))

print('Learned regression tree model:')
print(model.toDebugString())

# Save model
#model.save(sc, "decisionTreeRegressionModel")
