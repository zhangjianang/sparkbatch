package daily.cores.upgrades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2019/4/22.
 */
public class Cartesain {

    public static void main(String[] args) {
        SparkConf conf =new SparkConf()
                .setAppName("cartesian")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> name = new ArrayList<>();
        name.add("ang");
        name.add("小明");
        name.add("旺财");
        name.add("lili");
        JavaRDD<String> partOne = jsc.parallelize(name);
        List<String> name2 = new ArrayList<>();
        name2.add("夹克");
        name2.add("短裙");
        name2.add("丝袜");
        JavaRDD<String> partTwo = jsc.parallelize(name2);

        JavaPairRDD<String,String> alls = partOne.cartesian(partTwo);

        for(Tuple2<String,String> per:alls.collect()){
            System.out.println(per);
        }
        jsc.close();
    }
}
