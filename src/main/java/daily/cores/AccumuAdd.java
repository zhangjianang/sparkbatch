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
public class AccumuAdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("accuAdd")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numRdd = sc.parallelize(nums);

        List<Integer> closerNum = new ArrayList<>();
        closerNum.add(0);
        numRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                Integer close = closerNum.get(0);
                close += integer;
                closerNum.set(0,close);
                System.out.println("add :"+ close);
            }
        });

        System.out.println(closerNum.get(0));
        sc.close();
    }
}
