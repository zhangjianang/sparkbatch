package bi.prism;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Administrator on 2017/5/26.
 */
public class CreateIDFile {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("CreateIDFile").getOrCreate();
        spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.196:3306/prism1")
                .option("dbtable", "company")
                .option("user", "finder")
                .option("password", "commercial123").load().registerTempTable("company");

        Dataset<Row> load =spark.sql("select id from company where company_org_type!='个体户'");

        load.javaRDD().saveAsTextFile("hdfs://192.168.0.191:8020/data/companyid");

    }
}
