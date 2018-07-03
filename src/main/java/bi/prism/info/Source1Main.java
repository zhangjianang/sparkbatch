package bi.prism.info;


import com.alibaba.fastjson.JSONObject;
import bi.TimeTool;
import bi.postgresql.model.CoreConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

import static bi.prism.info.JsonTools.convertSpecialStr;


@SuppressWarnings("all")
//TODO
//company_category_20170411这个表可能会变
/**
 * 合并 company 和 company_category_20170411
 * 然后落地 hdfs://master:9000/sparktmp/main
 */
public class Source1Main {

    //private static String hiveDataBase = CoreConfig.HIVE_DATABASE;
//    private static String hdfsUrl = CoreConfig.HDFS_URL;

  /*  private static String url = "jdbc:mysql://172.31.11.43:3306/prism1?useUnicode=true&amp;characterEncoding=UTF-8";
    private static String user = "root";
    private static String password = "";*/


    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("SparkMySqlPrims")
                .enableHiveSupport()
                .config("spark.executor.memoryoverhead", "4096M")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions","500")
                .config("spark.default.parallelism","500")
                .getOrCreate();
        spark.sql(TimeTool.genDbInfo(args));

        //MainRDD
        JavaPairRDD<String, Iterable<String>> mainRDD = createMainRDD(spark);


        //合并主
        JavaPairRDD<String, String> converMainRDD = convertMain(mainRDD);


        converMainRDD.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1()+"~|~"+v1._2();
            }
        }).saveAsTextFile(TimeTool.genHdfsUrl()+"main");

    }



    private static JavaPairRDD<String, String> convertMain(JavaPairRDD<String, Iterable<String>> mainRDD) {


        JavaPairRDD<String, String> returnRDD = mainRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> v1) throws Exception {

                String companyId = v1._1();
                Iterator<String> mainIter = v1._2().iterator();
                Iterator<String> annualIter = null;


                List<Map<String, Object>> company = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> company_abnormal_info = new ArrayList<Map<String, Object>>();

                List<Map<String, Object>> company_branch = new ArrayList<Map<String, Object>>();

                List<Map<String, Object>> company_catetory = new ArrayList<Map<String, Object>>();


                List<Map<String, Object>> company_check_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> company_equity_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> company_investor = new ArrayList<Map<String, Object>>();

                List<Map<String, Object>> company_punishment_info = new ArrayList<Map<String, Object>>();

                String company_id = null;


                Map<String, Object> mainMap = null;

                while (mainIter.hasNext()) {

                    String strNext = mainIter.next();


                    Map<String, Object> next = JSONObject.parseObject(strNext, Map.class);

                    if ("company".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_id = next.get("id").toString();
                        company.add(next);
                    } else if ("company_abnormal_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_abnormal_info.add(next);
                    }  else if ("company_check_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_check_info.add(next);
                    } else if ("company_equity_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_equity_info.add(next);
                    } /*else if ("company_investor".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_investor.add(next);
                    } */else if ("company_punishment_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_punishment_info.add(next);
                    }/* else if ("company_staff".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_staff.add(next);
                    }*/ else if ("company_branch".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_branch.add(next);
                    } else if ("company_category".equalsIgnoreCase(next.get("tablename").toString())) {
                        company_catetory.add(next);
                    }
                }
                if (company.size() > 0) {
                    mainMap = company.get(0);
                    mainMap.put("checkList", company_check_info);
                    mainMap.put("comAbnoInfoList", company_abnormal_info);
                    mainMap.put("equityList", company_equity_info);
                   // mainMap.put("investorListAll", company_investor);
                    mainMap.put("punishList", company_punishment_info);
                   // mainMap.put("staffListAll", company_staff);
                    mainMap.put("branchList", company_branch);

                    if (company_catetory.size() > 0) {
                        Object categroy_code = company_catetory.get(0).get("category_code");
                        mainMap.put("category_code", categroy_code);
                    }
                }


                String jres = JSONObject.toJSONString(mainMap);
                return new Tuple2<String, String>(companyId, jres);
            }
        });


        return returnRDD;

    }

    private static JavaPairRDD<String,Iterable<String>> createMainRDD(SparkSession spark) {

       /* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company")
                .option("user", user)
                .option("driver", "com.mysql.jdbc.Driver")
                *//*.option("password", password)*//*.load().registerTempTable("company");*/

        Dataset<Row> load =spark.sql("select" +
                " actual_capital as actualCapital ,approved_time as approvedTime ,base,business_scope as businessScope ,id,company_org_type as companyOrgType ,company_type as companyType ,flag,from_time as fromTime,legal_person_id as legalPersonId ," +
                "legal_person_name as legalPersonName ,name,org_number as orgNumber,reg_capital as regCapital,reg_institute as regInstitute,reg_location as regLocation,reg_number as regNumber,reg_status as regStatus,source_flag as sourceFlag,updatetime ," +
                "to_time as toTime,legal_person_type as type ,property1 as creditCode , " +
                "parent_id" +
                " from "+CoreConfig.COMPANY_INCREASE);

        JavaPairRDD<String, String> companyrdd = load.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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
                returnMap.put("tablename", "company");


                Object regCapital = returnMap.get("regCapital");
                if(regCapital!=null){
                    Object[] objects = DealRegCapital.dealField(regCapital.toString());
                    if(objects!=null&&objects.length==2){
                        returnMap.put("registeredCapital", objects[0]);
                        returnMap.put("currency",objects[1] );
                    }else{
                        returnMap.put("registeredCapital", "");
                        returnMap.put("currency","");
                    }
                }else{
                    returnMap.put("registeredCapital", "");
                    returnMap.put("currency","");
                }

                String reJson = JSONObject.toJSONString(returnMap);

                Object id = returnMap.get("id");
                if(id==null){
                    id = "";
                }
                return new Tuple2<String, String>(id.toString(), reJson);
            }
        });


     /*   spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company")
                .option("user", user)
                .option("driver", "com.mysql.jdbc.Driver")
                *//*.option("password", password)*//*.load().registerTempTable("company_category");
*/

        Dataset<Row> load41 =spark.sql("select company_id,category_code from company_category_20170411");
        JavaPairRDD<String, String> company_category = load41.javaRDD().mapToPair(new PairFunction<Row, String, String>() {

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {
                    returnMap.put(fieldname, row.get(i));
                    i++;
                }
                //branchList
                returnMap.put("tablename", "company_category");
                String reJson = JSONObject.toJSONString(returnMap);


                Object companyId = returnMap.get("company_id");
                if (companyId == null) {
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





        JavaPairRDD<String, Iterable<String>> mainRDD = companyrdd
                .union(company_category)

                .filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> v1) throws Exception {
                        if(v1._1()==null||"".equalsIgnoreCase(v1._1())){
                            return Boolean.FALSE;
                        }
                        return Boolean.TRUE;
                    }
                }).groupByKey().filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Iterable<String>> v1) throws Exception {
                        Iterator<String> iterator = v1._2().iterator();



                        while (iterator.hasNext()) {
                            String next = iterator.next();
                            Map<String, Object> mapNext=JSONObject.parseObject(next,Map.class);
                            if ("company".equalsIgnoreCase(mapNext.get("tablename").toString())) {
                                return Boolean.TRUE;
                            }
                        }
                        return Boolean.FALSE;
                    }
                });

        return mainRDD;
    }
}
