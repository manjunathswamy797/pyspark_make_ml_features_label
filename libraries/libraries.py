from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("").getOrCreate()
SparkSession.builder.appName("").getOrCreate().sparkContext.setLogLevel("ERROR")
