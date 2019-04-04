package daily.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * Created by adimn on 2019/4/4.
 */
public class PersistWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf =new SparkConf()
                .setAppName("PersistTest")
                .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list","192.168.0.191:9092");
        Set<String> topics = new HashSet<>();
        topics.add("t_test");

        jsc.checkpoint("hdfs://192.168.0.191:8020/tmp/test/t_test");
        JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> input) throws Exception {
                return Arrays.asList(input._2.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String input) throws Exception {

                return new Tuple2<String, Integer>(input,1);
            }
        });

        JavaPairDStream<String, Integer> wordscount = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newvalue = 0;
                if (state.isPresent()) {
                    newvalue = state.get();
                } else {
                    for (Integer per : values) {
                        newvalue += per;
                    }
                }
                return Optional.of(newvalue);
            }
        });
        wordscount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
                wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> words) throws Exception {
                        Connection conn = ConnectionPool.getConnection();
                        Statement statement = conn.createStatement();
                        StringBuilder sql = new StringBuilder("insert into wordcount (word,count) values ");
                        while(words.hasNext()){
                            Tuple2<String, Integer> next = words.next();
                            sql.append("("+next._1+","+next._2+"),");
                        }
                        if(!",".equals(sql.charAt(sql.length()-1))){
                            return;
                        }
                        sql.deleteCharAt(sql.length()-1);
                        statement.execute(sql.toString());
                        ConnectionPool.returnConnection(conn);
                    }
                });
            }
        });


        jsc.start();
        jsc.awaitTermination();
        jsc.stop();

    }
}
