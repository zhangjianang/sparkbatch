package daily.cores.upgrades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2019/4/22.
 */
public class Intersection {

    public static void main(String[] args) {
        SparkConf conf =new SparkConf()
                .setAppName("intersection")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> name = new ArrayList<>();
        name.add("ang");
        name.add("小明");
        name.add("旺财");
        name.add("lili");
        JavaRDD<String> partOne = jsc.parallelize(name);
        List<String> name2 = new ArrayList<>();
        name2.add("lili");
        name2.add("harry");
        name2.add("one");
        JavaRDD<String> partTwo = jsc.parallelize(name2);

        JavaRDD<String> alls = partOne.intersection(partTwo);

        for(String per:alls.collect()){
            System.out.println(per);
        }
        jsc.close();
    }
}
