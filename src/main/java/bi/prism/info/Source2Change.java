package bi.prism.info;

import com.alibaba.fastjson.JSONObject;
import bi.TimeTool;
import bi.postgresql.model.CoreConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

import static bi.prism.info.JsonTools.convertSpecialStr;


@SuppressWarnings("ALL")
/**
 * 合并company_change_info
 * 落地 hdfs://master:9000/sparktmp/change
 */
public class Source2Change {
    private static String hiveDataBase = CoreConfig.HIVE_DATABASE;
//    private static String hdfsUrl = CoreConfig.HDFS_URL;

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("SparkMySqlPrims")
                .enableHiveSupport()
                .config("spark.executor.memoryoverhead", "2048M")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions","500")
                .config("spark.default.parallelism","500")
                .getOrCreate();

//        spark.sql(hiveDataBase);
        spark.sql(TimeTool.genDbInfo(args));

        JavaPairRDD<String, String> rdd = createRDD(spark.read().textFile(TimeTool.genHdfsUrl()+"main").javaRDD());

        JavaPairRDD<String, String> converMainChange = convertMainAndChangeInfo(rdd, spark);


        converMainChange.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1()+"~|~"+v1._2();
            }
        }).saveAsTextFile(TimeTool.genHdfsUrl()+"change");


    }

    private static JavaPairRDD<String,String> createRDD(JavaRDD<String> sourceRdd) {
        JavaPairRDD<String, String> returnRDD = sourceRdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split(Pattern.quote("~|~"));
                return new Tuple2<String, String>(split[0], split[1]);
            }
        });

        return returnRDD;
    }

    private static JavaPairRDD<String, String> convertMainAndChangeInfo(JavaPairRDD<String, String> mainRDD, SparkSession spark) {

        Dataset<Row> load2 =spark.sql("select id,company_id as companyId,change_item as changeItem,change_time as changeTime,content_after as contentAfter,content_before as contentBefore,createTime from company_change_info");

        JavaPairRDD<String, Iterable<String>> company_change_info = load2.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
                    i++;
                }
                returnMap.put("companyName", null);
                String reJson = JSONObject.toJSONString(returnMap);

                Object companyId = returnMap.get("companyId");
                if (companyId == null) {
                    companyId = "";
                }
                return new Tuple2<String, String>(companyId.toString(), reJson);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                if (v1._1() == null || "".equals(v1._1())) {
                    return false;
                }
                return true;
            }
        }).groupByKey();


        JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join = mainRDD.leftOuterJoin(company_change_info);

        JavaPairRDD<String, String> returnRDD = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Iterable<String>>>>, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Iterable<String>>>> v1) throws Exception {
                List<Map<String, Object>> company_change_info = new ArrayList<Map<String, Object>>();

                if (v1._2()._2().isPresent()) {
                    Iterable<String> changeIds = v1._2()._2().get();

                    if (changeIds != null) {
                        Iterator<String> iterator = changeIds.iterator();
                        while (iterator.hasNext()) {
                            String next = iterator.next();
                            Map map = JSONObject.parseObject(next, Map.class);
                            company_change_info.add(map);
                        }
                    }

                }


                Map<String, Object> mainMap = JSONObject.parseObject(v1._2()._1(), Map.class);

                mainMap.put("comChanInfoList", company_change_info);

                String jres = JSONObject.toJSONString(mainMap);

                return new Tuple2<String, String>(v1._1(), jres);

            }
        });


        return returnRDD;
    }

}
