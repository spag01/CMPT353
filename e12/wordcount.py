import sys
from pyspark.sql import SparkSession, functions, types, Row
import re
import numpy as np
import math
import string

spark = SparkSession.builder.appName('correlate logs').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation

def get_updated_data(data_file, data_split):
    data_file = data_split.select(functions.explode(data_split.words).alias('word')).cache()
    data_file = data_file.select(functions.lower(data_file.word).alias('word'))
    data_file = data_file.groupby('word').count()
    data_file = data_file.sort(functions.col('count').desc(), functions.col('word'))
    return data_file

def main(input_directory, output_directory):
    data_file = spark.read.text(input_directory)
    data_split = data_file.select(functions.split(data_file.value, wordbreak).alias('words'))

    data_file = get_updated_data(data_file, data_split)

    data_file = data_file.filter(data_file.word != "")
    data_file.write.csv(output_directory, mode='overwrite')

if __name__=='__main__':
    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    main(input_directory, output_directory) 