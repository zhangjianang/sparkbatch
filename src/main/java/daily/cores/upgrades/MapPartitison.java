package daily.cores.upgrades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;


import java.util.*;

/**
 * Created by adimn on 2019/4/19.
 */
public class MapPartitison {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("mapPartitions")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String > names = Arrays.asList("zhangone","王二","麻子","zhangtwo");
        JavaRDD<String> studentRdd = jsc.parallelize(names, 2);

        Map<String,Integer> scores = new HashMap<>();
        scores.put("zhangone",100);
        scores.put("王二",60);
        scores.put("麻子",70);
        scores.put("zhangtwo",90);

        // mapPartitions
        // 类似map，不同之处在于，map算子，一次就处理一个partition中的一条数据
        // mapPartitions算子，一次处理一个partition中所有的数据

        // 推荐的使用场景
        // 如果你的RDD的数据量不是特别大，那么建议采用mapPartitions算子替代map算子，可以加快处理速度
        // 但是如果你的RDD的数据量特别大，比如说10亿，不建议用mapPartitions，可能会内存溢出

        JavaRDD<Integer> parScore = studentRdd.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<String> stringIterator) throws Exception {
                List<Integer> res = new ArrayList<Integer>();
                String studentName = stringIterator.next();
                Integer studentScore = scores.get(studentName);
                res.add(studentScore);
                return res.iterator();
            }
        });

        for(Integer studentScore: parScore.collect()) {
            System.out.println(studentScore);
        }
        jsc.close();
    }
}
