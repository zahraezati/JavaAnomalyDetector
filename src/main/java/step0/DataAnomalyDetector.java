package step0;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;

/**
 * Created by zahra on 11/6/17.
 */
public class DataAnomalyDetector {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception{

        /*if (args.length < 2) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];*/

        String brokers = "localhost:9092";
        String topics = "cpu-usage-stream";

        // Create context with a 1 second batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingKafka").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        JavaDStream<String> lines = messages.map(ConsumerRecord::value);

        lines.foreachRDD(l -> System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>> " + l.toDebugString()));

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
