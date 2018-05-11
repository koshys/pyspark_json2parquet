from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from functools import lru_cache

# set the spark context
#  hadoop fs -put /Users/samson/Downloads/example.json JSONtoParquet/source/example.json

'''
  destination should always end in a forward slash
'''


@lru_cache(maxsize=1)
def get_conf():
    return {
        'pysparkapp': 'PysparkJson2Parquet',
        'host': 'local[*]',
        'out_block_sz': 1024**2,
        'destination': 's3://mybucket-data-lake/',
        'destination_qa': 's3://mybucket-qa-data-lake/',
        'destination_dev': 's3://mybucket-dev-data-lake/',
        'mem_cap_gb': 2.0
    }


@lru_cache(maxsize=1)
def _get_pyspark():

    c = get_conf()
    conf = SparkConf().setAppName(c['pysparkapp']).setMaster(c['host'])
    sc = SparkContext(conf=conf)
    return sc


'''
    sc -> SparkContext
'''
@lru_cache(maxsize=1)
def get_sql_context():

    sc = _get_pyspark()
    sql_context = SQLContext(sc)
    return sql_context


'''
    sc -> SparkContext
'''
@lru_cache(maxsize=1)
def get_pyspark_logger():

    sc = _get_pyspark()
    c = get_conf()
    sc._jsc.hadoopConfiguration().setInt("dfs.blocksize", c['out_block_sz'])
    sc._jsc.hadoopConfiguration().setInt("parquet.block.size", c['out_block_sz'])
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(c['pysparkapp'])
    return log
