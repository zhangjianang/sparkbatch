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
 * 合并Annual 多表
 * 落地hdfs://master:9000/sparktmp/mainreport
 * Created by Administrator on 2017/5/24.
 */
public class Source7MainReport {

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


        JavaPairRDD<String, String> rdd = createRDD(spark.read().textFile(TimeTool.genHdfsUrl()+"mainall").javaRDD());

        JavaPairRDD<String, Iterable<String>> annualRDD = createAnnualRDD(spark);

        //转换annual
        JavaPairRDD<String, Iterable<String>> newAnnualRDD = convertAnnualRDD(annualRDD);

        JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join = rdd.leftOuterJoin(newAnnualRDD);

        //第一次连接结果转换
        JavaPairRDD<String, String> mainAndAnnualRDD =  convertMainAndAnnual(join);

        mainAndAnnualRDD.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1()+"~|~"+v1._2();
            }
        }).saveAsTextFile(TimeTool.genHdfsUrl()+"mainreport");

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

    private static JavaPairRDD<String, String> convertMainAndAnnual(JavaPairRDD<String, Tuple2<String, Optional<Iterable<String>>>> join) {

        JavaPairRDD<String, String> returnRDD = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Iterable<String>>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Iterable<String>>>> v1) throws Exception {

                Map<String, Object> mainMap = JSONObject.parseObject(v1._2()._1(), Map.class);
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

    private static JavaPairRDD<String,Iterable<String>> createAnnualRDD(SparkSession spark) {


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
                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
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

                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
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

                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
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

                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
                    i++;
                }

                returnMap.put("tablename", "report_outbound_investment");
                String reJson = JSONObject.toJSONString(returnMap);
                Object annualreport_id = returnMap.get("annual_report_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });

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

                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
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

                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
                    i++;
                }

                returnMap.put("tablename", "report_shareholder");
                String reJson = JSONObject.toJSONString(returnMap);
                Object annualreport_id = returnMap.get("annual_report_id");
                if(annualreport_id==null){
                    annualreport_id = "";
                }

                return new Tuple2<String,String>(annualreport_id.toString(), reJson);
            }
        });


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

                    returnMap.put(fieldname, convertSpecialStr(row.get(i)));
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


}
