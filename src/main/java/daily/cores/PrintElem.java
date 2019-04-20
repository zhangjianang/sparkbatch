package daily.cores;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by adimn on 2019/4/19.
 */
public class PrintElem {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("printElem")
                .setMaster("local")
                .set("spark.default.parallelism","2");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5,7,8,9);
        JavaRDD<Integer> numRdd = sc.parallelize(nums);

        numRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(""+ integer);
            }
        });

        sc.close();
    }
}
