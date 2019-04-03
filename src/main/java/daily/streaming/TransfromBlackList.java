package daily.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by adimn on 2019/4/3.
 */
public class TransfromBlackList {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("AngBlackList")
                .setMaster("Local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","192.168.0.191:9092");
        Set<String> topics = new HashSet<>();
        topics.add("t_test");
        JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        ArrayList<Tuple2<String, Boolean>> blacklist = new ArrayList<>();
        blacklist.add(new Tuple2<>("tom",true));
        final  JavaPairRDD<String, Boolean> blackName = jsc.sparkContext().parallelizePairs(blacklist);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> input) throws Exception {
                return Arrays.asList(input._2.split(" ")).iterator();
            }
        });
        JavaPairDStream<String,String> pairs = words.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[1],s);
            }
        });

        pairs.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> pairs) throws Exception {
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRdd = pairs.leftOuterJoin(blackName);
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filter = joinRdd.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if(tuple._2._2().isPresent() && tuple._2._2.get()){
                            return false;
                        }
                        return true;
                    }
                });
                JavaRDD<String> validAdsClickLogRDD = filter.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });
                return validAdsClickLogRDD;
            }
        });


        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
