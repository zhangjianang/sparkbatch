package bi.prism.info;

import bi.postgresql.model.CoreConfig;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Administrator on 2017/9/15 0015.
 */
public class TestHive {

    private static String hiveDataBase = CoreConfig.HIVE_DATABASE;
    private static String hdfsUrl = CoreConfig.HDFS_URL;

    public static void main(String[] args) {


     /*  SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setAppName("SparkMySqlPrims");
        //conf.set("spark.executor.memoryoverhead","2048M");
        conf.set("spark.sql.shuffle.partitions","200");
        conf.set("spark.default.parallelism","200");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "AKIAPATACE4K42FVRMNQ");
        sc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "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");
        sc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn");
        String logFile = "s3a://bigdata-xsy/person.json";

        sc.textFile(logFile).foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 2458549230266380991L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        sc.close();
*/

        SparkSession spark = SparkSession.builder().appName("SparkMySqlPrims")
                .enableHiveSupport()
                .config("spark.executor.memoryoverhead", "2048M")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.default.parallelism", "200")
                .getOrCreate();



        spark.sql(hiveDataBase);

        spark.sql("select * from company_illegal_info").javaRDD().saveAsTextFile(hdfsUrl+"company_illegal_info");







       /*SparkSession spark = SparkSession.builder().appName("SparkMySqlPrims")
                //.enableHiveSupport()
                .config("spark.executor.memoryoverhead", "2048M")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.default.parallelism", "200")
                .config("fs.s3a.awsAccessKeyId", "AKIAPATACE4K42FVRMNQ")
                .config("fs.s3a.awsSecretAccessKey", "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")
                .config("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn")
                .getOrCreate();


         spark.read().textFile("s3a://bigdata-xsy/person.json").javaRDD().foreach(new VoidFunction<String>() {
             private static final long serialVersionUID = 2458549230266380991L;

             @Override
             public void call(String s) throws Exception {
                 System.out.println("**************************"+s+"***************************************************");
             }
         });
*/

        //spark.sql("use prism1");
       // System.out.println(spark.sql("select count(*) from mortgage_change_info").javaRDD().count());

   /*    spark.conf.set("fs.s3a.awsAccessKeyId", "AKIAPATACE4K42FVRMNQ");
       spark.conf.set("fs.s3a.awsSecretAccessKey", "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");
       spark.conf.set("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn");

        spark.sql("use prism1");

        spark.conf().set();*/

        //RDD<String> stringRDD = spark.sparkContext().textFile("s3a://bigdata-xsy/person.json",2);

        //System.out.println(stringRDD.count());*/

      /*  URL resource = Thread.currentThread().getContextClassLoader().getResource("person.json");
        Dataset<Row> person = spark.read().json("D:\\ideawork\\spark2.0-project\\src\\main\\resources\\person.json");

        person.javaRDD().saveAsTextFile("s3://bigdata-xsy/test");
*/


         //System.out.println(spark.sql("select count(*) from mortgage_change_info").javaRDD().count());


    }

}
