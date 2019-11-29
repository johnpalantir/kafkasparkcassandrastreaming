package com.palantir.KafkaSparkCassandraStreaming.streams;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkStreamKafkaCassandApp {

    private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SparkStreamKafkaCassandApp.class);
    private static int TIME_TO_LIVE_SECONDS = 24 * 60 * 365 * 2;



    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org")
                .setLevel(Level.OFF);
        Logger.getLogger("akka")
                .setLevel(Level.OFF);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group-test");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("hello");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("WordCountingApp");
        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
        sparkConf.set("spark.cassandra.connection.port", "9042");


        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> results = messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));



        JavaDStream<String> lines = results.map(tuple2 -> tuple2._2());

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+"))
                .iterator());

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);



         wordCounts.foreachRDD(javaRdd -> {
                Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
                for (String key : wordCountMap.keySet()) {
                    List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
                    JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);

                    RDDAndDStreamCommonJavaFunctions<Word>.WriterBuilder writerBuilder = javaFunctions(rdd).writerBuilder("vocabulary", "words", mapToRow(Word.class, getApiData()));

                    writerBuilder.saveToCassandra();

                    javaFunctions(rdd).writerBuilder("vocabulary", "words", mapToRow(Word.class, getApiData()))
                            .withConstantTTL(TIME_TO_LIVE_SECONDS)
                            .withConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                            .saveToCassandra();
                    System.out.println(rdd.first());
                    System.out.println("End");

                }
            });


        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static Map<String,String> getApiData() {

        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("word","word");
        columnNameMappings.put("count","count");

        return columnNameMappings;
    }
}