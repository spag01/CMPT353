import sys
import numpy as np
import math
import re

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('correlate logs').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred.
    Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        # TODO
        return Row(host_name = m.group(1), number_of_bytes = int(m.group(2)))
    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)
    # TODO: return an RDD of Row() objects
    RDD_of_row = log_lines.map(line_to_row)
    RDD_of_row = RDD_of_row.filter(not_none)
    return RDD_of_row


def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory)).cache()
    groupA = logs.groupby("hostname").count()
    groupB = logs.groupby("hostname").sum("bytes")
    data_point = groupA.join(groupB, "hostname").cache()
    data_point = data_point.withColumn('xi^2', data_point['count'] * data_point['count'])
    data_point = data_point.withColumn('yi^2', data_point['sum(bytes)'] * data_point['sum(bytes)'])
    data_point = data_point.withColumn('xi*yi', data_point['sum(bytes)'] * data_point['count'])
    n = data_point.count()
    data_point = data_point.groupby().sum()
    numerator_value1 = (n * data_point.first()['sum(xi*yi)'])
    numerator_value2 = (data_point.first()['sum(count)'] * data_point.first()['sum(sum(bytes))'])
    denominator_value1 = math.sqrt( (n * data_point.first()['sum(xi^2)'] ) - (data_point.first()['sum(count)'] * data_point.first()['sum(count)']) )
    denominator_value2 = math.sqrt( (n * data_point.first()['sum(yi^2)'] ) - (data_point.first()['sum(sum(bytes))'] * data_point.first()['sum(sum(bytes))']))

    # TODO: calculate r.

    r = ( numerator_value1 - numerator_value2)/( denominator_value1 * denominator_value2)# TODO: it isn't zero.
    print(f"r = {r}\nr^2 = {r*r}")

    # Built-in function should get the same results.
    #print(totals.corr('count', 'bytes'))


if __name__=='__main__':
    in_directory = sys.argv[1]
    main(in_directory)

