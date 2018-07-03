package bi.prism.info;


import com.alibaba.fastjson.JSONObject;
import bi.TimeTool;
import bi.postgresql.model.CoreConfig;
import bi.prism.CategroyCache;
import bi.prism.ConvertTool;
import bi.prism.MD5;
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
 * 合并Mortgage 多表
 * 落地 hdfs://master:9000/sparktmp/mainreport
 * Created by Administrator on 2017/5/24.
 */
public class Source8Final {

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


        JavaPairRDD<String, String> rdd = createRDD(spark.read().textFile(TimeTool.genHdfsUrl()+"mainreport").javaRDD());


        //创建MortgageRDD
        JavaPairRDD<String, Iterable<String>> mortgageRDD = createMortgageRDD(spark);

        //转换mortgage
        JavaPairRDD<String, Iterable<String>> newMortgageRDD =  convertMortgageRDD(mortgageRDD);

        JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join2 = rdd.leftOuterJoin(newMortgageRDD);

        //最终RDD
        JavaPairRDD<String, String> finalRDD = convertFinalModelRDD(join2);

        finalRDD.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1()+"~|~"+v1._2();
            }
        }).saveAsTextFile(TimeTool.genHdfsUrl()+"final");


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

    private static JavaPairRDD<String, String> convertFinalModelRDD(JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join2) {

        JavaPairRDD<String, String> returnRdd = join2.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Iterable<String>>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Iterable<String>>>> v1) throws Exception {
                {
                    String key = v1._1();

                    Map map = JSONObject.parseObject(v1._2()._1(), Map.class);

                    List<Map<String, Object>> mortgageList = new ArrayList<Map<String, Object>>();
                    Iterator<String> mortgages = null;
                    if (v1._2()._2().isPresent()) {
                        mortgages = v1._2()._2().get().iterator();
                    }

                    if (mortgages != null) {
                        while (mortgages.hasNext()) {
                            mortgageList.add(JSONObject.parseObject(mortgages.next(), Map.class));
                        }
                    }

                    map.put("mortgageList", mortgageList);
                    Object name = map.get("name");

                    if (name == null) {
                        name = "none";
                    }

                    Map<String, String> categoryMap = CategroyCache.getCategroyMap();


                    Object category_code = map.get("category_code");
                    if (category_code == null) {
                        map.put("category_code", "");
                        map.put("industry", "");
                    } else {
                        Object industry = categoryMap.get(category_code);
                        if (industry == null) {
                            map.put("industry", "");
                        } else {
                            map.put("industry", industry.toString());
                        }

                    }

                    map.put("companyUrl", "");
                    map.put("illegalList", "");
                    map.put("investList", "");
                    map.put("keyword", "");
                    map.put("percentileScore", "");
                    map.put("creditCode", map.get("property1"));
                    map.put("updateTimes", map.get("updatetime"));
                    map.put("term", map.get("fromTime") + "至" + map.get("toTime"));
                    map.put("type", map.get("legal_person_type"));
                    map.put("correctCompanyId", map.get("id"));//id
                    map.put("categoryScore", "");
                    map.put("companyUrl", "");
                    map.put("keyword", "");
                    map.put("percentileScore", "");


                    String json = JSONObject.toJSONString(map);

                    String cname = MD5.MD5_32bit(name.toString());

                    return new Tuple2<String, String>(cname, json);
                }
            }
        });


        return returnRdd;
    }


    private static JavaPairRDD<String,Iterable<String>> convertMortgageRDD(JavaPairRDD<String, Iterable<String>> mortRDD) {
        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = mortRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> v1) throws Exception {


                List<Map<String, Object>> company_mortgage_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> mortgage_change_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> mortgage_pawn_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> mortgage_people_info = new ArrayList<Map<String, Object>>();

                Map<String,Object> mortgage=null;
                String company_id = "";

                Iterator<String> iterator = v1._2.iterator();
                while (iterator.hasNext()) {
                    String strNext=iterator.next();
                    Map<String, Object> next = JSONObject.parseObject(strNext,Map.class);

                    if ("company_mortgage_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        if(next.get("company_id")!=null){
                            company_id = next.get("company_id").toString();
                        }
                        company_mortgage_info.add(next);
                    } else if ("mortgage_change_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        mortgage_change_info.add(next);
                    } else if ("mortgage_pawn_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        mortgage_pawn_info.add(next);
                    } else if ("mortgage_people_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        mortgage_people_info.add(next);
                    }
                }

                if(company_mortgage_info.size()>0){
                    mortgage=company_mortgage_info.get(0);
                    mortgage.put("mchangelist",mortgage_change_info);
                    mortgage.put("mpawnlist",mortgage_pawn_info);
                    mortgage.put("mpeoplelist",mortgage_people_info);
                }

                String strRes=JSONObject.toJSONString(mortgage);

                return new Tuple2<>(company_id, strRes);
            }
        }).groupByKey().filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<String>> v1) throws Exception {
                if(v1._1()==null||"".equalsIgnoreCase(v1._1())){
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            }
        });
        return stringIterableJavaPairRDD;
    }

    private static JavaPairRDD<String,Iterable<String>> createMortgageRDD(SparkSession spark) {

        Dataset<Row> load20 =spark.sql("select company_id,id,amount,base,id,overview_term as overviewTerm,publish_date as publishDate,reg_date as regDate,reg_department as regDepartment,reg_num as regNum,remark,scope,status,type from company_mortgage_info");
        JavaPairRDD<String, String> company_mortgage_info = load20.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();
                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname,convertSpecialStr(row.get(i)));
                    i++;
                }
                if(returnMap!=null&&returnMap.get("base")!=null) {
                    returnMap.put("province", ConvertTool.code2province.get(returnMap.get("base").toString().toUpperCase()));//需要转化
                }else{
                    returnMap.put("province", "");//需要转化
                }
                returnMap.put("tablename", "company_mortgage_info");

                String reJson = JSONObject.toJSONString(returnMap);


                Object id = returnMap.get("id");
                if(id==null){
                    id = "";
                }

                return new Tuple2<String, String>(id.toString(), reJson);
            }
        });

        Dataset<Row> load21 =spark.sql("select mortgage_id,change_content as changeContent,change_date as changeDate from mortgage_change_info");
        JavaPairRDD<String,String> mortgage_change_info = load21.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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
                returnMap.put("tablename", "mortgage_change_info");

                String reJson = JSONObject.toJSONString(returnMap);

                Object mortgage_id = returnMap.get("mortgage_id");
                if(mortgage_id==null){
                    mortgage_id = "";
                }
                return new Tuple2<String, String>(mortgage_id.toString(), reJson);
            }
        });

        Dataset<Row> load22 =spark.sql("select mortgage_id,detail,ownership,pawn_name as pawnName from mortgage_pawn_info");

        JavaPairRDD<String, String> mortgage_pawn_info = load22.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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

                returnMap.put("tablename", "mortgage_pawn_info");

                String reJson = JSONObject.toJSONString(returnMap);
                Object mortgage_id = returnMap.get("mortgage_id");
                if(mortgage_id==null){
                    mortgage_id = "";
                }
                return new Tuple2<String, String>(mortgage_id.toString(), reJson);
            }
        });

        Dataset<Row> load23 =spark.sql("select mortgage_id,license_num as licenseNum,license_type as liceseType,people_name as peopleName from mortgage_people_info");
        JavaPairRDD<String, String> mortgage_people_info = load23.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
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

                returnMap.put("tablename", "mortgage_people_info");

                String reJson = JSONObject.toJSONString(returnMap);
                Object mortgage_id = returnMap.get("mortgage_id");
                if(mortgage_id==null){
                    mortgage_id = "";
                }
                return new Tuple2<String, String>(mortgage_id.toString(), reJson);
            }
        });


        JavaPairRDD<String, Iterable<String>> mortgageRDD = company_mortgage_info.union(mortgage_change_info)
                .union(mortgage_pawn_info)
                .union(mortgage_people_info).filter(new Function<Tuple2<String, String>, Boolean>() {
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
                            String strNext=iterator.next();
                            Map<String, Object> next = JSONObject.parseObject(strNext,Map.class);
                            if ("company_mortgage_info".equalsIgnoreCase(next.get("tablename").toString())) {
                                return Boolean.TRUE;
                            }
                        }
                        return Boolean.FALSE;
                    }
                });
        return  mortgageRDD;
    }

}
