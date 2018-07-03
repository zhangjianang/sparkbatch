package bi.prism.info;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Created by Administrator on 2017/9/25 0025.
 */
public class TestSpark {
    public static void main(String args[]){
        SparkSession spark = SparkSession.builder().appName("SparkMySqlPrims")
                .enableHiveSupport()
                .config("spark.executor.memoryoverhead", "2048M")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.default.parallelism", "200")
                .getOrCreate();


        long count = spark.read().load("hdfs://192.168.0.191:8020/u20170917.sql").javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = 5155486753052237179L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return  new Tuple2<String,Integer>(row.mkString(),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -261865124598396923L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).count();

        System.out.println("共有效条数:"+count);
    }
}
