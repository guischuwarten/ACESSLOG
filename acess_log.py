# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
 
if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
 
    sparkConf = SparkConf()
    sparkConf.setAppName('Acess_log')
    sparkConf.set('spark.sql.parquet.compression.codec', 'snappy')
    sc = SparkContext(conf=sparkConf)
    
    #Lê arquivo acess_log
    rdd = sc.textFile("/user/gui_schuwarten/acess_log/")
    
    #Cria data frame
    df = sqlContext.createDataFrame(rdd, StringType())
    
    
    df = df.select(regexp_extract('value', '([^ ]*)', 1).alias('host'), regexp_extract('value', ' - - \[([^\]]*)\]', 1).alias('data'), regexp_extract('value', ' "(.*?)"', 1).alias('requisicao'), substring(regexp_extract('value', '([0-9]".*)', 1), 4, 3).alias('http'), substring(regexp_extract('value', '([0-9]".*)', 1), 8, 4).alias('bytes'))
    
    #Resposta 1
    df.agg(countDistinct('host')).show()
    
    #Resposta 2
    df.where("http == '404'").groupby('http').count().show()

    #Resposta 3
    df.where("http == '404'").groupby('http', 'requisicao').count().orderBy(desc('count')).show(5, False)
    
    #Resposta 4
    df.where("http == '404'").groupby('http', 'data').count().show()
    
    #Resposta 5
    df.agg(count('bytes')).show()
