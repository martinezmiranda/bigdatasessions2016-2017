from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils
from numpy import array

conf = SparkConf().setAppName('mldemo')
sc = SparkContext(conf=conf)
spark = HiveContext(sc)


# Load and parse the data file into an DataFrame via HiveQL
#FILL_HERE

def parse_raw(line):
    #FILL_HERE
    return LabeledPoint(#FILL_HERE)

#Convertir los rdd en LabeledPoint
#FILL_HERE

# Entrenar el modelo con un test de decision
#FILL_HERE

# Evaluar el modelo
#FILL_HERE


#Fusionar las predicciones y el dataset de test y generar CSV
#FILL_HERE (Primero, definir funcion, luego generar CSV)
def toCSVLine(data):
  return #FILL_HERE

#Guardar el CSV
csv.saveAsTextFile("csvexport")

# Save model
model.save(sc, "decisionTreeRegressionModel")


print('Learned regression tree model:')
print(model.toDebugString())
