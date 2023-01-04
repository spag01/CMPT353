import sys
from pyspark.sql import SparkSession, functions, types
import re

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')


# spark with version greater or equal to 2.3 and python with 3.5
assert sys.version_info >= (3, 5)
assert spark.version >= '2.3'

wiki_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('requests', types.LongType()),
    types.StructField('bytes', types.LongType()),
   
])

def get_date(path):
    x = path.split('-')
    return x[2] + '-' + x[3][0] + x[3][1]

def get_pgcnt(in_d):
    pgcnt = spark.read.csv(in_d, sep = ' ' , schema = wiki_schema ).withColumn('filename', functions.input_file_name())
    pgcnt = pgcnt.filter(pgcnt.language == 'en')
    pgcnt = pgcnt.filter(pgcnt.title != 'Main_Page')
    pgcnt= pgcnt.filter(pgcnt.title.startswith('Special:') == False)
    
    converted = functions.udf(lambda z: get_date(z), types.StringType() )
    pgcnt = pgcnt.withColumn('date_hour', converted(pgcnt.filename) )
    
    return pgcnt

def get_grp(pgcnt):
    grp = pgcnt.groupBy('date_hour').max('requests')
    grp = grp.withColumnRenamed('max(requests)','requests')
    grp.cache();

    return grp
    
def main(in_d, out_d):
    pgcnt = get_pgcnt(in_d)
    grp = get_grp(pgcnt)
    res = grp.join(pgcnt, ['date_hour','requests'])
    res = res.sort('date_hour','title')
    res = res.drop('language','bytes','filename')
    res = res.select('date_hour','title','requests')
    res.write.csv(out_d, mode='overwrite')


if __name__=='__main__':
    in_d = sys.argv[1]
    out_d = sys.argv[2]
    main(in_d, out_d)
