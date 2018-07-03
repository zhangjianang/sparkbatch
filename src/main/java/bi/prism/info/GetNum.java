package bi.prism.info;


import com.alibaba.fastjson.JSONObject;
import bi.prism.CategroyCache;
import bi.prism.ConvertTool;
import bi.prism.MD5;
import bi.prism.ResultModel;
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


@SuppressWarnings("ALL")
/**
 * Created by Administrator on 2017/5/24.
 */
public class GetNum {


    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("SparkMySqlPrims")
                //.master("local")
                .enableHiveSupport()
                .config("spark.executor.memoryoverhead", "2048M")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               // .config("hive.exec.dynamic.partition.mode","nonstrict")
               // .config("hive.exec.dynamic.partition","true")
                .config("spark.sql.shuffle.partitions","500")
                .config("spark.default.parallelism","500")
                .getOrCreate();

        spark.sql("use prism1");


        System.out.println(spark.read().textFile("hdfs://master:9000/sparktmp/inverstor").javaRDD().count());

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

    private static JavaPairRDD<String,String> converMainAndInverstor(JavaPairRDD<String, String> converMainStaff, SparkSession spark) {
        Dataset<Row> load8 =spark.sql("select a.company_id, a.amount,b.id,b.name,b.type from company_investor as a LEFT JOIN human as b on a.investor_id=b.id");

        JavaPairRDD<String, Iterable<String>> company_investor = load8.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "company_investor");
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
                if (v1._1() == null || "".equals(v1._1())) {
                    return false;
                }
                return true;
            }
        }).groupByKey();


        JavaPairRDD<String, Tuple2<String, Iterable<String>>> join = converMainStaff.join(company_investor);
        JavaPairRDD<String, String> returnRDD = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Iterable<String>>>, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Iterable<String>>> v1) throws Exception {
                List<Map<String, Object>> company_staff = new ArrayList<Map<String, Object>>();
                Iterable<String> changeIds = v1._2()._2();

                if (changeIds != null) {
                    Iterator<String> iterator = changeIds.iterator();
                    while (iterator.hasNext()) {
                        String next = iterator.next();
                        Map map = JSONObject.parseObject(next, Map.class);
                        company_staff.add(map);
                    }
                }


                Map<String, Object> mainMap = JSONObject.parseObject(v1._2()._1(), Map.class);

                mainMap.put("investorListAll", company_staff);

                String jres = JSONObject.toJSONString(mainMap);

                return new Tuple2<String, String>(v1._1(), jres);
            }
        });


        return returnRDD;

    }

    private static JavaPairRDD<String,String> converMainAndStaff(JavaPairRDD<String, String> converMainChange, SparkSession spark) {


        Dataset<Row> load9 = spark.sql("select company_staff.company_id ,human.id ,name,type,staff_type_name as typeJoin from company_staff left join human on company_staff.staff_id = human.id");
        JavaPairRDD<String, Iterable<String>> staff = load9.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "company_staff");
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
                if (v1._1() == null || "".equals(v1._1())) {
                    return false;
                }
                return true;
            }
        }).groupByKey();


        JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join = converMainChange.leftOuterJoin(staff);


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

                mainMap.put("staffListAll", company_change_info);

                String jres = JSONObject.toJSONString(mainMap);

                return new Tuple2<String, String>(v1._1(), jres);

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

                    returnMap.put(fieldname, row.get(i));
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


        JavaPairRDD<String, Tuple2<String, Iterable<String>>> join = mainRDD.join(company_change_info);
        JavaPairRDD<String, String> returnRDD = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Iterable<String>>>, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Iterable<String>>> v1) throws Exception {
                List<Map<String, Object>> company_change_info = new ArrayList<Map<String, Object>>();
                Iterable<String> changeIds = v1._2()._2();

                if (changeIds != null) {
                    Iterator<String> iterator = changeIds.iterator();
                    while (iterator.hasNext()) {
                        String next = iterator.next();
                        Map map = JSONObject.parseObject(next, Map.class);
                        company_change_info.add(map);
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

    private static JavaRDD<String> convertFinalRDD(JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join2) {
        JavaRDD<String> returnRdd = join2.map(new Function<Tuple2<String, Tuple2<String, Optional<Iterable<String>>>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<String, Optional<Iterable<String>>>> v1) throws Exception {
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


                return MD5.MD5_32bit(name.toString()) + "|" + json;
            }


        });

        return returnRdd;
    }




    private static JavaRDD<ResultModel> convertFinalModelRDD(JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join2) {
        JavaRDD<ResultModel> returnRdd = join2.map(new Function<Tuple2<String, Tuple2<String, Optional<Iterable<String>>>>, ResultModel>() {
            @Override
            public ResultModel call(Tuple2<String, Tuple2<String, Optional<Iterable<String>>>> v1) throws Exception {
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

                return new ResultModel(cname,json);
            }


        });

        return returnRdd;
    }


    private static void convertMainAndAnnualAndMortgage(JavaPairRDD<String, Tuple2<String, Iterable<String>>> join2) {
        JavaPairRDD<String, String> stringStringJavaPairRDD = join2.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Iterable<String>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Iterable<String>>> v1) throws Exception {
                String key = v1._1();


                Map map = JSONObject.parseObject(v1._2()._1(), Map.class);
                Iterator<String> mortIterator =v1._2()._2().iterator();


                map.put("mortgageList",mortIterator);



                String json =  JSONObject.toJSONString( map );
                return  null;
            }
        });



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

    private static JavaPairRDD<String, String> convertMainAndAnnual(JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join) {

        JavaPairRDD<String, String> returnRDD = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Iterable<String>>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Iterable<String>>>> v1) throws Exception {

                Map<String, Object> mainMap = JSONObject.parseObject(v1._1(), Map.class);
                Iterator<String> annualIter = null;
                if (v1._2()._2().isPresent()) {
                    annualIter = v1._2()._2().get().iterator();
                }

                List<Map<String, Object>> annualReportList = new ArrayList<Map<String, Object>>();
                if (annualIter != null) {

                    while (annualIter.hasNext()) {
                        String strNext = annualIter.next();
                        Map<String, Object> next = JSONObject.parseObject(strNext, Map.class);
                        annualReportList.add(next);
                    }

                }

                mainMap.put("annualReportList", annualReportList);

                String jres = JSONObject.toJSONString(mainMap);
                return new Tuple2<String, String>(v1._1(), jres);
            }
        });


        return returnRDD;
    }

    private static JavaPairRDD<String,Iterable<String>> convertAnnualRDD(JavaPairRDD<String, Iterable<String>> annualRDD) {
        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = annualRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> v1) throws Exception {


                List<Map<String, Object>> annual_report = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> report_change_record = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> report_equity_change_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> report_out_guarantee_info = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> report_outbound_investment = new ArrayList<Map<String, Object>>();

                List<Map<String, Object>> report_shareholder = new ArrayList<Map<String, Object>>();
                List<Map<String, Object>> report_webinfo = new ArrayList<Map<String, Object>>();

                Map<String,Object> annualReport =new HashMap<String, Object>();

                String company_id = "";
                Iterator<String> iterator = v1._2.iterator();
                while (iterator.hasNext()) {
                    String strNext=iterator.next();
                    //取出数据进行转换
                    Map<String, Object> next = JSONObject.parseObject(strNext,Map.class);


                    if ("annual_report".equalsIgnoreCase(next.get("tablename").toString())) {
                        if(next.get("company_id")!=null){
                            company_id = next.get("company_id").toString();
                        }
                        annual_report.add(next);
                    } else if ("report_change_record".equalsIgnoreCase(next.get("tablename").toString())) {
                        report_change_record.add(next);
                    } else if ("report_equity_change_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        report_equity_change_info.add(next);
                    } else if ("report_out_guarantee_info".equalsIgnoreCase(next.get("tablename").toString())) {
                        report_out_guarantee_info.add(next);
                    } else if ("report_outbound_investment".equalsIgnoreCase(next.get("tablename").toString())) {
                        report_outbound_investment.add(next);
                    } else if ("report_shareholder".equalsIgnoreCase(next.get("tablename").toString())) {
                        report_shareholder.add(next);
                    } else if ("report_webinfo".equalsIgnoreCase(next.get("tablename").toString())) {
                        report_webinfo.add(next);
                    }
                }

                if(annual_report!=null&&annual_report.size()>0) {
                    annualReport.put("baseInfo", annual_report.get(0) );
                }else{
                    annualReport.put("baseInfo",null);
                }

                annualReport.put("changeRecordList",report_change_record);
                annualReport.put("equityChangeInfoList",report_equity_change_info);
                annualReport.put("outboundInvestmentList",report_outbound_investment);
                annualReport.put("outGuaranteeInfoList",report_out_guarantee_info);
                annualReport.put("shareholderList",report_shareholder);
                annualReport.put("webInfoList",report_webinfo);
                String jres=JSONObject.toJSONString(annualReport);
                return new Tuple2<>(company_id, jres);
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


      /*  spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_mortgage_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_mortgage_info");*/


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

                    returnMap.put(fieldname, row.get(i));
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


/*
        spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "mortgage_change_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("mortgage_change_info");*/


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

                    returnMap.put(fieldname, row.get(i));
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


     /*  spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "mortgage_pawn_info")
                .option("user", user)
               .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("mortgage_pawn_info");
*/

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

                    returnMap.put(fieldname, row.get(i));
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


        /*spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "mortgage_people_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("mortgage_people_info");*/

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

                    returnMap.put(fieldname, row.get(i));
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

    private static JavaPairRDD<String,Iterable<String>> createAnnualRDD(SparkSession spark) {
       /* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "annual_report")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("annual_report");*/

        Dataset<Row> load10 =spark.sql("select company_id,id,company_name,credit_code,email,employee_num,manage_state,operator_name,phone_number,postal_address,postcode,prime_bus_profit" +
                ",reg_number,report_year,retained_profit,total_assets,total_equity,total_liability,total_profit,total_sales,total_tax from annual_report");
        JavaPairRDD<String, String> annual_report = load10.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String,String> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {
                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                Map<String,Object> dataResult=new HashMap<String, Object>();
                dataResult.put("id",returnMap.get("id"));
                dataResult.put("company_id",returnMap.get("company_id"));
                dataResult.put("companyName", returnMap.get("company_name"));
                dataResult.put("creditCode", returnMap.get("credit_code"));
                dataResult.put("email", returnMap.get("email"));
                dataResult.put("employeeNum", returnMap.get("employee_num"));
                dataResult.put("manageState", returnMap.get("manage_state"));
                dataResult.put("operatorName", returnMap.get("operator_name"));
                dataResult.put("phoneNumber", returnMap.get("phone_number"));
                dataResult.put("postalAddress", returnMap.get("postal_address"));
                dataResult.put("postcode", returnMap.get("postcode"));
                dataResult.put("primeBusProfit", returnMap.get("prime_bus_profit"));
                dataResult.put("regNumber", returnMap.get("reg_number"));
                dataResult.put("reportYear", returnMap.get("report_year"));
                dataResult.put("retainedProfit", returnMap.get("retained_profit"));
                dataResult.put("totalAssets", returnMap.get("total_assets"));
                dataResult.put("totalEquity", returnMap.get("total_equity"));
                dataResult.put("totalLiability", returnMap.get("total_liability"));
                dataResult.put("totalProfit", returnMap.get("total_profit"));
                dataResult.put("totalSales", returnMap.get("total_sales"));
                dataResult.put("totalTax", returnMap.get("total_tax"));
                dataResult.put("tablename", "annual_report");
                String reJson = JSONObject.toJSONString(dataResult);


                Object id = returnMap.get("id");
                if(id==null){
                    id = "";
                }

                return new Tuple2<String, String>(id.toString(), reJson);
            }
        });

     /*   spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "report_change_record")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("report_change_record");
*/

        Dataset<Row> load11 =spark.sql("select annualreport_id,change_item as changeItem,change_time as changeTime,content_after as contentAfter,content_before as contentBefore from report_change_record ");
        JavaPairRDD<String,String> report_change_record = load11.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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
                returnMap.put("tablename", "report_change_record");
                String reJson = JSONObject.toJSONString(returnMap);

                Object annualreport_id = returnMap.get("annualreport_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });


/*
        spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "report_equity_change_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("report_equity_change_info");*/


        Dataset<Row> load12 =spark.sql("select annualreport_id, change_time as changeTime,investor_name as investorName,ratio_after as ratioAfter,ratio_before as ratioBefore from report_equity_change_info");
        JavaPairRDD<String, String> report_equity_change_info = load12.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "report_equity_change_info");
                String reJson = JSONObject.toJSONString(returnMap);
                Object annualreport_id = returnMap.get("annualreport_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });



      /* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "report_outbound_investment")
                .option("user", user)
               .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("report_outbound_investment");*/

        Dataset<Row> load13 =spark.sql("select annual_report_id,credit_code as creditCode,outcompany_name as outcompanyName,reg_num as regNum from report_outbound_investment");
        JavaPairRDD<String, String> report_outbound_investment = load13.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "report_outbound_investment");
                String reJson = JSONObject.toJSONString(returnMap);
                Object annualreport_id = returnMap.get("annualreport_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });



      /* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "report_out_guarantee_info")
                .option("user", user)
               .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("report_out_guarantee_info");*/

        Dataset<Row> load14 =spark.sql("select annualreport_id,creditor,credito_amount as credito_amount,credito_term as creditoTerm,credito_type as creditoType,guarantee_scope as guaranteeScope,guarantee_term as guaranteeTerm,guarantee_way as guaranteeWay,obligor from report_out_guarantee_info");
        JavaPairRDD<String, String> report_out_guarantee_info = load14.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "report_out_guarantee_info");
                String reJson = JSONObject.toJSONString(returnMap);
                Object annualreport_id = returnMap.get("annualreport_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });


      /* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "report_shareholder")
                .option("user", user)
               .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("report_shareholder");
*/
        Dataset<Row> load15 = spark.sql("select annual_report_id,investor_name as investorName,paid_amount as paidAmount,paid_time as paidTime,paid_type as paidType,subscribe_amount as subscribeAmount,subscribe_time as subscribeTime,subscribe_type as subscribeType from report_shareholder");
        JavaPairRDD<String, String> report_shareholder = load15.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "report_shareholder");
                String reJson = JSONObject.toJSONString(returnMap);
                Object annualreport_id = returnMap.get("annualreport_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });



       /*spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "report_webinfo")
                .option("user", user)
               .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("report_webinfo");*/


        Dataset<Row> load16 = spark.sql("select annualreport_id, web_type as webType,name,website from report_webinfo");
        JavaPairRDD<String, String> report_webinfo = load16.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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
                returnMap.put("tablename", "report_webinfo");
                String reJson = JSONObject.toJSONString(returnMap);
                Object annualreport_id = returnMap.get("annualreport_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });


        return annual_report.union(report_change_record)
                .union(report_equity_change_info)
                .union(report_outbound_investment)
                .union(report_out_guarantee_info)
                .union(report_shareholder)
                .union(report_webinfo)
                .filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> v1) throws Exception {
                        if(v1._1()==null||"".equalsIgnoreCase(v1._1())){
                            return Boolean.FALSE;
                        }
                        return Boolean.TRUE;
                    }
                })

                .groupByKey().filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Iterable<String>> v1) throws Exception {
                        Iterator<String> iterator = v1._2().iterator();

                        while (iterator.hasNext()) {
                            String strNext=iterator.next();
                            Map<String, Object> next = JSONObject.parseObject(strNext,Map.class);
                            if ("annual_report".equalsIgnoreCase(next.get("tablename").toString())) {
                                return Boolean.TRUE;
                            }
                        }
                        return Boolean.FALSE;
                    }
                });
    }


    private static JavaPairRDD<String,Iterable<String>> createMainRDD(SparkSession spark) {




        /*spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company")
                .option("user", user)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company");*/

        Dataset<Row> load =spark.sql("select" +
                " actual_capital as actualCapital ,approved_time as approvedTime ,base,business_scope as businessScope ,id,company_org_type as companyOrgType ,company_type as companyType ,flag,from_time as fromTime,legal_person_id as legalPersonId ," +
                "legal_person_name as legalPersonName ,name,org_number as orgNumber,reg_capital as regCapital,reg_institute as regInstitute,reg_location as regLocation,reg_number as regNumber,reg_status as regStatus,source_flag as sourceFlag,updatetime ," +
                "to_time as toTime,legal_person_type as type ,property1 as creditCode , " +
                "parent_id" +
                " from company where company_org_type!='个体户'");

        JavaPairRDD<String, String> companyrdd = load.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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
                returnMap.put("tablename", "company");

                String reJson = JSONObject.toJSONString(returnMap);

                Object id = returnMap.get("id");
                if(id==null){
                    id = "";
                }
                return new Tuple2<String, String>(id.toString(), reJson);
            }
        });


        //根据company和category构造branchList
      /* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_category_20170411")
                .option("user", user)
               .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_category_20170411");
*/
        Dataset<Row> load41 =spark.sql("select company_id,category_code from company_category_20170411 ");
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



       /* Dataset<Row> parent_df =spark.sql("select" +
                " name as childname,reg_number as regNumber,reg_status as regStatus,legal_person_name as legalPersonName ,parent_id " +
                " from company where company_org_type!='个体户' and parent_id <> 0 and parent_id is not null");

        JavaPairRDD<String, String> branchrdd = parent_df.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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
                returnMap.put("tablename", "company_branch");
                String reJson = JSONObject.toJSONString(returnMap);

                Object parent_id = returnMap.get("parent_id");
                if(parent_id==null){
                    parent_id = "";
                    returnMap.put("parent_id",parent_id);
                }

                return new Tuple2<String, String>(parent_id.toString(), reJson);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                if(v1._1()==null||"".equals(v1._1())){
                    return true;
                }else{
                    return false;
                }
            }
        });*/





      /*  spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_change_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_change_info");*/


       /* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_abnormal_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_abnormal_info");*/


        /*Dataset<Row> load3 =spark.sql("select id,company_id as companyId,createTime as createtime,put_date as putDate  ,put_department as putDepartment,put_reason as putReason,remove_date as removeDate,remove_department as removeDepartment,remove_reason as removeReason from company_abnormal_info");

        JavaPairRDD<String, String> company_abnormal_info = load3.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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



       *//* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_check_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_check_info");*//*


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

                    returnMap.put(fieldname, row.get(i));
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




       *//* spark.read().format("jdbc")

                .option("url", url)
                .option("dbtable", "company_equity_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_equity_info");*//*

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

                    returnMap.put(fieldname, row.get(i));
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



       *//* spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_punishment_info")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_punishment_info");*//*


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

                    returnMap.put(fieldname, row.get(i));
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
        });*/


     /*   spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "human")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("human");



        spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_investor")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_investor");*/


        /*Dataset<Row> load8 =spark.sql("select a.company_id, a.amount,b.id,b.name,b.type from company_investor as a LEFT JOIN human as b on a.investor_id=b.id");

        JavaPairRDD<String, String> company_investor = load8.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "company_investor");
                String reJson = JSONObject.toJSONString(returnMap);
                Object companyId = returnMap.get("company_id");
                if(companyId==null){
                    companyId = "";
                }
                return new Tuple2<String, String>(companyId.toString(), reJson);
            }
        });*/



       /*spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "company_staff")
                .option("user", user)
                .option("driver","com.mysql.jdbc.Driver")
                .option("password", password).load().registerTempTable("company_staff");*/

      /*  Dataset<Row> load9 = spark.sql("select company_staff.company_id ,human.id ,name,type,staff_type_name as typeJoin from company_staff left join human on company_staff.staff_id = human.id");
        JavaPairRDD<String, String> company_staff = load9.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -1964392616303257605L;

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

                returnMap.put("tablename", "company_staff");
                String reJson = JSONObject.toJSONString(returnMap);
                Object companyId = returnMap.get("company_id");
                if(companyId==null){
                    companyId = "";
                }
                return new Tuple2<String, String>(companyId.toString(), reJson);
            }
        });*/


        JavaPairRDD<String, Iterable<String>> mainRDD = companyrdd/*.union(company_change_info)*/
               // .union(company_abnormal_info)
                .union(company_category)
                //.union(company_check_info)
                //.union(company_equity_info)
                //.union(company_punishment_info)
               // .union(company_investor)
               // .union(company_staff)
                //.union(branchrdd)
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
