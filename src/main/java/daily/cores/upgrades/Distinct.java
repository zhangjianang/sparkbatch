package daily.cores.upgrades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2019/4/22.
 */
public class Distinct {

    public static void main(String[] args) {
        SparkConf conf =new SparkConf()
                .setAppName("distinct")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> name = new ArrayList<>();
        name.add("ang 10.10.1.1");
        name.add("小明");
        name.add("旺财");
        name.add(null);
        name.add("lili");
        name.add("lili");
        name.add("lili");
        name.add("");
        name.add(" ");
        JavaRDD<String> partOne = jsc.parallelize(name);
        List<String> name2 = new ArrayList<>();
        name2.add("lili");
        name2.add("harry");
        name2.add("one");
        JavaRDD<String> partTwo = jsc.parallelize(name2);
        JavaRDD<String> filter = partOne.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                if("".equals(v1)|| " ".equals(v1) || v1 == null){
                    return false;
                }
                return true;
            }
        });
        JavaRDD<String> map = filter.map(new Function<String, String>() {
            @Override
            public String call(String input) throws Exception {
                return input.split(" ")[0];
            }
        });
        JavaRDD<String> alls = map.distinct();

        for(String per:alls.collect()){
            System.out.println(per);
        }
        jsc.close();
    }
}
