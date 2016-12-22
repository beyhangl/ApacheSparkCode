#Here get data from Apache Kafka and it's calculatin word count streaming
##beyhan gul


from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark_cassandra
from pyspark_cassandra import streaming
from datetime import datetime

# Create a StreamingContext with batch interval of 3 second
conf=SparkConf().setAppName("myapp").set("spark.cassandra.connection.host", "127.0.0.1")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 3)

topic = "topic_cassandraam"
kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "topic", {topic: 4})



lines = kafkaStream.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

# save to cassandra
counts.saveToCassandra("keyspace", "mytable")

ssc.start()             
ssc.awaitTermination()
