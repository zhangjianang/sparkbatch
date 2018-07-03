package bi.prism;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Created by Administrator on 2017/9/12 0012.
 */
public class SparkCapital {


    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("PrimsInputHdfs")
                //.master("local")
                .enableHiveSupport()
                .config("spark.executor.memoryoverhead", "2048M")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.exec.dynamic.partition", "true")
                .getOrCreate();

        spark.sql("use prism1");

        JavaRDD<Row> rdd = spark.sql("select id,reg_capital from company").javaRDD();

        rdd.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -4750971879973514812L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String,String>(row.getString(0),row.getString(1));
            }
        }).saveAsTextFile("hdfs://192.168.0.191:9000/data/1.data");
        
    }
}
