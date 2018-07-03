package bi.prism.info;


import com.alibaba.fastjson.JSONObject;
import bi.TimeTool;
import bi.postgresql.model.CoreConfig;
import bi.prism.ConvertTool;
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
 * 合并 company 的其他表
 * 落地 hdfs://master:9000/sparktmp/mainall
 * Created by Administrator on 2017/5/24.
 */
public class Source6CompanyOther {

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

        spark.sql(TimeTool.genDbInfo(args));

        JavaPairRDD<String, String> rdd = createRDD(spark.read().textFile(TimeTool.genHdfsUrl()+"branch").javaRDD());

        JavaPairRDD<String, Iterable<String>> mainRDD = createMainRDD(spark);

        JavaPairRDD<String, String> mainall = converMainOther(rdd,mainRDD);

        mainall.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1()+"~|~"+v1._2();
            }
        }).saveAsTextFile(TimeTool.genHdfsUrl()+"mainall");
    }

    private static JavaPairRDD<String,String> converMainOther(JavaPairRDD<String, String> rdd, JavaPairRDD<String, Iterable<String>> mainOther) {

        JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join = rdd.leftOuterJoin(mainOther);

        JavaPairRDD<String, String> returnRDD = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Iterable<String>>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Iterable<String>>>> v1) throws Exception {

                String companyId = v1._1();

                List<Map<String, Object>> company_abnormal_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> company_check_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> company_equity_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> company_punishment_info = new ArrayList<Map<String, Object>>();


                if (v1._2()._2().isPresent()) {
                    Iterator<String> mainIter = v1._2()._2().get().iterator();
                    while (mainIter.hasNext()) {

                        String strNext = mainIter.next();


                        Map<String, Object> next = JSONObject.parseObject(strNext, Map.class);

                        if ("company_abnormal_info".equalsIgnoreCase(next.get("tablename").toString())) {
                            company_abnormal_info.add(next);
                        } else if ("company_check_info".equalsIgnoreCase(next.get("tablename").toString())) {
                            company_check_info.add(next);
                        } else if ("company_equity_info".equalsIgnoreCase(next.get("tablename").toString())) {
                            company_equity_info.add(next);
                        } else if ("company_punishment_info".equalsIgnoreCase(next.get("tablename").toString())) {
                            company_punishment_info.add(next);
                        }
                    }

                }


                Map<String, Object> mainMap = JSONObject.parseObject(v1._2()._1(), Map.class);

                mainMap.put("checkList", company_check_info);
                mainMap.put("comAbnoInfoList", company_abnormal_info);
                mainMap.put("equityList", company_equity_info);
                mainMap.put("punishList", company_punishment_info);


                String jres = JSONObject.toJSONString(mainMap);
                return new Tuple2<String, String>(companyId, jres);
            }
        });


        return returnRDD;
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


    private static JavaPairRDD<String,Iterable<String>> createMainRDD(SparkSession spark) {


        Dataset<Row> load3 =spark.sql("select id,company_id as companyId,createTime as createtime,put_date as putDate  ,put_department as putDepartment,put_reason as putReason,remove_date as removeDate,remove_department as removeDepartment,remove_reason as removeReason from company_abnormal_info");

        JavaPairRDD<String, String> company_abnormal_info = load3.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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

                returnMap.put("tablename", "company_abnormal_info");
                String reJson = JSONObject.toJSONString(returnMap);


                Object companyId = returnMap.get("companyId");
                if(companyId==null){
                    companyId = "";
                }
                return new Tuple2<String, String>(companyId.toString(), reJson);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                if(v1._1()==null || "".equals(v1._1())){
                    return false;
                }
                return true;
            }
        });




        Dataset<Row> load5 =spark.sql("select company_id,check_org as checkOrg,check_type as checkType ,check_date as checkDate,check_result as checkResult from company_check_info");
        JavaPairRDD<String, String> company_check_info = load5.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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

                returnMap.put("tablename", "company_check_info");
                String reJson = JSONObject.toJSONString(returnMap);
                Object companyId = returnMap.get("company_id");
                if(companyId==null){
                    companyId = "";
                }
                return new Tuple2<String, String>(companyId.toString(), reJson);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                if(v1._1()==null || "".equals(v1._1())){
                    return false;
                }
                return true;
            }
        });





        Dataset<Row> load6 = spark.sql("select company_id,base,certif_number_l as certifNumber,certif_number_r as certifNumberR,equity_amount as equityAmount,pledgee,pledgor,reg_date as regDate,reg_number as regNumber,state from company_equity_info");

        JavaPairRDD<String, String> company_equity_info = load6.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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
                if(returnMap.get("base")!=null&&returnMap.get("base")!=null) {
                    returnMap.put("province", ConvertTool.code2province.get(returnMap.get("base").toString().toUpperCase()));
                }else{
                    returnMap.put("province","");
                }
                returnMap.put("tablename", "company_equity_info");
                String reJson = JSONObject.toJSONString(returnMap);
                Object companyId = returnMap.get("company_id");
                if(companyId==null){
                    companyId = "";
                }
                return new Tuple2<String, String>(companyId.toString(), reJson);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                if(v1._1()==null || "".equals(v1._1())){
                    return false;
                }
                return true;
            }
        });




        Dataset<Row> load7 =spark.sql("select company_id,base,content,department_name as departmentName,name,punish_number as punishNumber ,type from company_punishment_info");
        JavaPairRDD<String, String> company_punishment_info = load7.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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
                if( returnMap.get("base")!=null&&returnMap.get("base")!=null) {
                    returnMap.put("province", ConvertTool.code2province.get(returnMap.get("base").toString().toUpperCase()));
                }else{
                    returnMap.put("province","");
                }

                returnMap.put("tablename", "company_punishment_info");
                String reJson = JSONObject.toJSONString(returnMap);
                Object companyId = returnMap.get("company_id");
                if(companyId==null){
                    companyId = "";
                }
                return new Tuple2<String, String>(companyId.toString(), reJson);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                if(v1._1()==null || "".equals(v1._1())){
                    return false;
                }
                return true;
            }
        });



        JavaPairRDD<String, Iterable<String>> mainRDD =
                company_abnormal_info
                        .union(company_check_info)
                        .union(company_equity_info)
                        .union(company_punishment_info)
                .filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> v1) throws Exception {
                        if(v1._1()==null||"".equalsIgnoreCase(v1._1())){
                            return Boolean.FALSE;
                        }
                        return Boolean.TRUE;
                    }
                }).groupByKey();

        return mainRDD;
    }
}
