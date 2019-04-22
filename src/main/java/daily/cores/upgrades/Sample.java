package daily.cores.upgrades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Created by adimn on 2019/4/19.
 */
public class Sample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("samples")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String > names = Arrays.asList("zhangone","王二","麻子","zhangtwo","zhangthree","小明","旺财","福贵");
        JavaRDD<String> studentRdd = jsc.parallelize(names, 2);
        // sample算子
        // 可以使用指定的比例，比如说0.1或者0.9，从RDD中随机抽取10%或者90%的数据
        // 从RDD中随机抽取数据的功能
        // 推荐不要设置第三个参数，feed
        JavaRDD<String> sample = studentRdd.sample(false, 0.3);


        for(String studentWithClass : sample.collect()) {
            System.out.println(studentWithClass);
        }

        jsc.close();
    }
}
