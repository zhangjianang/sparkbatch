package daily.streaming;


import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by adimn on 2019/4/3.
 */
public class UpdateStateByKeyWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("UpdateStateByKey")
                .setMaster("Local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        jsc.checkpoint("hdfs://192.168.0.191:8020/tmp/test/t_test");
        JavaReceiverInputDStream<String> lines =jsc.socketTextStream("localhost",9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String str) throws Exception {
                return Arrays.asList(str.split(" ")).iterator();
            }
        });

        JavaPairDStream<String,Integer> pair =  words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String str) throws Exception {
                return new Tuple2<String, Integer>(str,1);
            }
        });
        //		 到了这里，就不一样了，之前的话，是不是直接就是pairs.reduceByKey
//		 然后，就可以得到每个时间段的batch对应的RDD，计算出来的单词计数
//		 然后，可以打印出那个时间段的单词计数
//		 但是，有个问题，你如果要统计每个单词的全局的计数呢？
//		 就是说，统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么就之前的方式就不好实现
//		 就必须基于redis这种缓存，或者是mysql这种db，来实现累加
//
//		 但是，我们的updateStateByKey，就可以实现直接通过Spark维护一份每个单词的全局的统计次数

        // 这里的Optional，相当于Scala中的样例类，就是Option，可以这么理解
        // 它代表了一个值的存在状态，可能存在，也可能不存在
        JavaPairDStream<String, Integer> wordcount = pair.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            // 这里两个参数
            // 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
            // 第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
            // 比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
            // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
            @Override
            public Optional<Integer> call(List<Integer> valules, Optional<Integer> state) throws Exception {
                Integer newvalue = 0;
                if(state.isPresent()){
                    newvalue = state.get();
                }
                for(Integer per:valules){
                    newvalue += per;
                }
                return Optional.of(newvalue);
            }
        });
        // 到这里为止，相当于是，每个batch过来是，计算到pairs DStream，就会执行全局的updateStateByKey
        // 算子，updateStateByKey返回的JavaPairDStream，其实就代表了每个key的全局的计数
        // 打印出来
        wordcount.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();

    }
}
