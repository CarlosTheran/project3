from pyspark import SparkConf, SparkContext

from pyspark.streaming import StreamingContext

from pyspark.sql import Row, SparkSession

from pyspark.ml.feature import Tokenizer

from pyspark.ml.feature import StopWordsRemover

from pyspark.ml.feature import Word2Vec

from pyspark.ml.classification import LogisticRegression

from pyspark.streaming.kafka import KafkaUtils

from kafka import SimpleProducer, KafkaClient

from kafka import KafkaProducer

import html


import gensim, logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
 
 
import os
import csv

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'


try:

    import json

except ImportError:

    import simplejson as json


def getSparkSessionInstance(sparkConf):

    if ('sparkSessionSingletonInstance' not in globals()):

        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()

    return globals()['sparkSessionSingletonInstance']



def consumer():

    context = StreamingContext(sc, 240)
    dStream = KafkaUtils.createDirectStream(context, ["sentimental_analysis"], {"metadata.broker.list": "localhost:9092"})

    dStream.foreachRDD(sentimental_analysis)

    context.start()

    context.awaitTermination()

def sentimental_analysis(time,rdd):
    
    rdd=rdd.map(lambda x: json.loads(x[1]))

    text_array=rdd.collect()

    text_array = [element["text"].lower() for element in text_array if "text" in element]

    rdd = sc.parallelize(text_array)

    rdd = rdd.map(lambda x: x.replace(',',' ')).map(lambda x: x.replace('/',' ')).map(lambda x: x.replace('?',' ')).map(lambda x: x.replace('...',' ')).map(lambda x: x.replace('-',' ')) 
   
    rdd = rdd.map(lambda x: x.replace('.',' ')).map(lambda x: x.replace('(',' ')).map(lambda x: x.replace(')',' ')).map(lambda x: x.replace('!',' ')).map(lambda x: x.replace('|',' '))

    rdd = rdd.map(lambda sn: ' '.join(filter(lambda x: x.startswith(('@','http','"','&','rt')) == False, sn.split() )))
  
    tweets_MAGA = rdd.filter(lambda x: "maga" in x).map(lambda x: [x, "MAGA"])
    
    tweets_DICTATOR = rdd.filter(lambda x: "dictator" in x).map(lambda x: [x, "DICTATOR"]) 

    tweets_IMPEACH = rdd.filter(lambda x: "impeach" in x).map(lambda x: [x, "IMPEACH"]) 
  
    tweets_DRAIN = rdd.filter(lambda x: "drain" in x).map(lambda x: [x,"DRAIN"])

    tweets_SWAMP = rdd.filter(lambda x: "swamp" in x).map(lambda x: [x, "SWAMP"])

    tweets_COMEY = rdd.filter(lambda x: "comey" in x).map(lambda x: [x,"COMEY"])

    tweets = tweets_DICTATOR.union(tweets_IMPEACH).union(tweets_DRAIN ).union(tweets_SWAMP).union(tweets_COMEY).union(tweets_MAGA)

    set_tweets = tweets.map(lambda x: Row(sentence=str.strip(x[0]), label = x[1], date_time = time))

    spark = getSparkSessionInstance(rdd.context.getConf())
 
    partsDF = spark.createDataFrame(set_tweets)

    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
   
    tokenized = tokenizer.transform(partsDF)

    remover = StopWordsRemover(inputCol="words", outputCol="base_words") #define parameter of StopWordsRemover funtion

    base_words = remover.transform(tokenized)

    train_data_row = base_words.select("base_words", "label", "date_time")

    word2vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features") 

    model = word2vec.fit(train_data_row)

    final_train_data = model.transform(train_data_row)

    resul_analysis = classifier.transform(final_train_data)

    resul_analysis = resul_analysis.select("label", "date_time", "prediction")

    resul_analysis.createOrReplaceTempView("sentimental_analysis")
   
    resul_analysisDF = spark.sql("select label, date_time, prediction, count(*) as total_label from sentimental_analysis group by label, date_time, prediction order by total_label")
   
    resul_analysisDF.write.mode("append").saveAsTable("sentimental_analysis")

    #resul_analysisDF.show()

    #resul_analysis.show()
    
    

def training_data():

   rdd = sc.textFile("/user/sentimental_analysis/Subset100k.csv")

   #header = data.first()

   #rdd = data.filter(lambda row: row != header)
   
   r = rdd.mapPartitions(lambda x: csv.reader(x))

   #r2 = r.map(lambda x: (x[3], int(x[1])))

   part = r.map(lambda x: Row(sentence=str.strip(x[3]),label=int(x[1]))) #put a schema and make data Frame 

   spark = getSparkSessionInstance(rdd.context.getConf())

   partsDF = spark.createDataFrame(part)

   #partDF.show()

   tokenizer = Tokenizer(inputCol="sentence", outputCol="words")  #define parameter of Tokenizer funtion

   tokenized = tokenizer.transform(partsDF)  # tokenizer split the sentences by row

   #tokenized.show()

   remover = StopWordsRemover(inputCol="words", outputCol="base_words") #define parameter of StopWordsRemover funtion

   base_words = remover.transform(tokenized)

   #base_words.show()

   train_data_row = base_words.select("base_words", "label")

   word2vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features") 

   model = word2vec.fit(train_data_row)

   final_train_data = model.transform(train_data_row)

   #final_train_data.show()

   final_train_data = final_train_data.select("label","features")

   lr = LogisticRegression(maxIter=10000, regParam=0.001, elasticNetParam=0.0001)

   lrModel = lr.fit(final_train_data)

   #lrModel.transform(final_train_data).show()

   return lrModel
#------------------------------------------------ Validacion Data2 ---------------------------------------#

   rdd = sc.textFile("/user/sentimental_analysis/Subset100k.csv")

   r = rdd.mapPartitions(lambda x: csv.reader(x))

   part = r.map(lambda x: Row(sentence=str.strip(x[3]),label=int(x[1])))

   spark = getSparkSessionInstance(rdd.context.getConf())
 
   partsDF = spark.createDataFrame(part)

   tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
   
   tokenized = tokenizer.transform(partsDF)

   remover = StopWordsRemover(inputCol="words", outputCol="base_words") #define parameter of StopWordsRemover funtion

   base_words = remover.transform(tokenized)

   train_data_row = base_words.select("base_words", "label")

   word2vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features") 

   model = word2vec.fit(train_data_row)

   final_train_data = model.transform(train_data_row)

   final_train_data = final_train_data.select("label","features")

   #lrModel.transform(final_train_data).show()

    

   print("********************************** Todo bajo control ***********************************")
   






if __name__ == "__main__":

    print("Starting CSV")

    sc = SparkContext(appName="Training_set")

    classifier = training_data()
    consumer()
    

    