import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from confluent_kafka import Consumer, Producer, KafkaException
from datetime import datetime

def process_batch(df, epoch_id):
    # Traitement des votes par candidat
    votes_per_candidate = df.groupBy("candidate_id", "candidate_name", "party_affiliation", "photo_url") \
        .agg(_sum("vote").alias("total_votes"))
    
    # Traitement des votes par location
    turnout_by_location = df.groupBy("address.state").count()

    # Écrire les résultats dans Kafka
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    # Envoyer les résultats par candidat
    for row in votes_per_candidate.collect():
        producer.produce(
            'aggregated_votes_per_candidate',
            key=row['candidate_id'],
            value=json.dumps({
                'candidate_id': row['candidate_id'],
                'candidate_name': row['candidate_name'],
                'party_affiliation': row['party_affiliation'],
                'photo_url': row['photo_url'],
                'total_votes': row['total_votes']
            })
        )

    # Envoyer les résultats par location
    for row in turnout_by_location.collect():
        producer.produce(
            'aggregated_turnout_by_location',
            key=row['state'],
            value=json.dumps({
                'state': row['state'],
                'count': row['count']
            })
        )

    producer.flush()

if __name__ == "__main__":
    # Configuration de base
    os.environ['HADOOP_HOME'] = r"C:\hadoop"
    os.environ['PATH'] = os.environ['PATH'] + r";C:\hadoop\bin"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Créer la SparkSession
    spark = (SparkSession.builder
             .appName("ElectionAnalysis")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "1")
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")

    # Définition du schéma
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

    # Consumer Kafka configuration
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'spark-vote-processor',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['votes_topic'])

    try:
        while True:
            # Collecter un batch de messages
            messages = []
            for _ in range(5):  # Traiter par lots de 100 messages
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                messages.append(json.loads(msg.value().decode('utf-8')))

            if messages:
                # Créer un DataFrame à partir des messages
                df = spark.createDataFrame(messages)
                
                # Traiter le batch
                process_batch(df, 0)

    except KeyboardInterrupt:
        print("Arrêt gracieux du streaming...")
    finally:
        consumer.close()
        spark.stop()