package daily.cores.upgrades;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Created by adimn on 2019/4/19.
 */
public class MapPartitisonsWithIndex {
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

        // 这里，parallelize并行集合的时候，指定了numPartitions是2
        // 也就是说，四个同学，会被分成2个班
        // 但是spark自己判定怎么分班

        // 如果你要分班的话，就必须拿到班级号
        // mapPartitionsWithIndex这个算子来做，这个算子可以拿到每个partition的index
        // 也就可以作为我们的班级号

        JavaRDD<String> stringJavaRDD = studentRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> studentWithClassList = new ArrayList<String>();

                while (iterator.hasNext()) {
                    String studentName = iterator.next();
                    String studentWithClass = studentName + "_" + (index + 1);
                    studentWithClassList.add(studentWithClass);
                }

                return studentWithClassList.iterator();
            }
        }, true);
        for(String studentWithClass : stringJavaRDD.collect()) {
            System.out.println(studentWithClass);
        }

        jsc.close();
    }
}
