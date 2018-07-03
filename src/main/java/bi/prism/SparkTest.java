package bi.prism;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;


@SuppressWarnings("ALL")
/**
 * Created by Administrator on 2017/5/24.
 */
public class SparkTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("SparkTest")
                                     .master("local").getOrCreate();

        JavaPairRDD<String, Iterable<Map<String, Object>>> mainRDD = createMainRDD(spark);

        JavaPairRDD<String, Iterable<Map<String, Object>>> annualRDD = createAnnualRDD(spark);

        JavaPairRDD<String, Iterable<Map<String, Object>>> mortgageRDD = createMortgageRDD(spark);

        annualRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Map<String,Object>>>, String, Map<String,Object>>() {
            @Override
            public Tuple2<String, Map<String, Object>> call(Tuple2<String, Iterable<Map<String, Object>>> v1) throws Exception {



                if(v1._1().equalsIgnoreCase("52")){
                    System.out.println("");
                }

                List<Map<String,Object>> annual_report = new ArrayList<Map<String,Object>>();
                List<Map<String,Object>> report_change_record = new ArrayList<Map<String,Object>>();
                List<Map<String,Object>> report_equity_change_info = new ArrayList<Map<String,Object>>();
                List<Map<String,Object>> report_out_guarantee_info = new ArrayList<Map<String,Object>>();
                List<Map<String,Object>> report_outbound_investment = new ArrayList<Map<String,Object>>();

                List<Map<String,Object>> report_shareholder = new ArrayList<Map<String,Object>>();
                List<Map<String,Object>> report_webinfo = new ArrayList<Map<String,Object>>();


                String company_id = "";
                Iterator<Map<String, Object>> iterator = v1._2.iterator();
                while(iterator.hasNext()){
                    Map<String, Object> next = iterator.next();

                    if("annual_report".equalsIgnoreCase(next.get("tablename").toString())){
                        company_id = next.get("company_id").toString();
                        annual_report.add(next);
                    }else if("report_change_record".equalsIgnoreCase(next.get("tablename").toString())){
                        report_change_record.add(next);
                    }else if("report_equity_change_info".equalsIgnoreCase(next.get("tablename").toString())){
                        report_equity_change_info.add(next);
                    }else if("report_out_guarantee_info".equalsIgnoreCase(next.get("tablename").toString())){
                        report_out_guarantee_info.add(next);
                    }else if("report_outbound_investment".equalsIgnoreCase(next.get("tablename").toString())){
                        report_outbound_investment.add(next);
                    }else if("report_shareholder".equalsIgnoreCase(next.get("tablename").toString())){
                        report_shareholder.add(next);
                    }else if("report_webinfo".equalsIgnoreCase(next.get("tablename").toString())){
                        report_webinfo.add(next);
                    }

                }



                System.out.println("");


                return new Tuple2<>(company_id,null);
            }
        }).count();




        System.out.println();


    }

    private static JavaPairRDD<String,Iterable<Map<String,Object>>> createMortgageRDD(SparkSession spark) {


        Dataset<Row> load20 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_mortgage_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_mortgage_info = load20.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_mortgage_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("id").toString(), returnMap);
            }
        });



        Dataset<Row> load21 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "mortgage_change_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> mortgage_change_info = load21.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "mortgage_change_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("mortgage_id").toString(), returnMap);
            }
        });


        Dataset<Row> load22 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "mortgage_pawn_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> mortgage_pawn_info = load22.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "mortgage_pawn_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("mortgage_id").toString(), returnMap);
            }
        });


        Dataset<Row> load23 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "mortgage_people_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> mortgage_people_info = load23.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "mortgage_people_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("mortgage_id").toString(), returnMap);
            }
        });


        JavaPairRDD<String, Iterable<Map<String, Object>>> mortgageRDD = company_mortgage_info.union(mortgage_change_info)
                .union(mortgage_pawn_info)
                .union(mortgage_people_info).groupByKey();
        return  mortgageRDD;
    }

    private static JavaPairRDD<String,Iterable<Map<String,Object>>> createAnnualRDD(SparkSession spark) {
        Dataset<Row> load10 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "annual_report")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> annual_report = load10.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "annual_report");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("id").toString(), returnMap);
            }
        });

        Dataset<Row> load11 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "report_change_record")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> report_change_record = load11.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "report_change_record");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("annualreport_id").toString(), returnMap);
            }
        });



        Dataset<Row> load12 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "report_equity_change_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> report_equity_change_info = load12.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "report_equity_change_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("annualreport_id").toString(), returnMap);
            }
        });



        Dataset<Row> load13 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "report_outbound_investment")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> report_outbound_investment = load13.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "report_outbound_investment");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("annual_report_id").toString(), returnMap);
            }
        });



        Dataset<Row> load14 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "report_out_guarantee_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> report_out_guarantee_info = load14.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "report_out_guarantee_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("annualreport_id").toString(), returnMap);
            }
        });


        Dataset<Row> load15 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "report_shareholder")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> report_shareholder = load15.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "report_shareholder");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("annual_report_id").toString(), returnMap);
            }
        });



        Dataset<Row> load16 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "report_webinfo")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> report_webinfo = load16.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "report_webinfo");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("annualreport_id").toString(), returnMap);
            }
        });


         return annual_report.union(report_change_record)
                .union(report_equity_change_info)
                .union(report_outbound_investment)
                .union(report_out_guarantee_info)
                .union(report_shareholder)
                .union(report_webinfo).groupByKey().filter(new Function<Tuple2<String, Iterable<Map<String, Object>>>, Boolean>() {
                     @Override
                     public Boolean call(Tuple2<String, Iterable<Map<String, Object>>> v1) throws Exception {
                         Iterator<Map<String, Object>> iterator = v1._2().iterator();

                         while (iterator.hasNext()) {
                             Map<String, Object> next = iterator.next();
                             if ("annual_report".equalsIgnoreCase(next.get("tablename").toString())) {
                                 return Boolean.TRUE;
                             }
                         }


                         return Boolean.FALSE;
                     }
                 });
    }

    private static JavaPairRDD<String,Iterable<Map<String,Object>>> createMainRDD(SparkSession spark) {

        Dataset<Row> load = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> companyrdd = load.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("id").toString(), returnMap);
            }
        });



        Dataset<Row> load2 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_change_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_change_info = load2.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_change_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });


        Dataset<Row> load3 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_abnormal_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_abnormal_info = load3.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_abnormal_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });



        Dataset<Row> load4 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_category")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_category = load4.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_category");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });



        Dataset<Row> load5 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_check_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_check_info = load5.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = null;

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_check_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });




        Dataset<Row> load6 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_equity_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_equity_info = load6.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_equity_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });



        Dataset<Row> load7 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_punishment_info")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_punishment_info = load7.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_punishment_info");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });



        Dataset<Row> load8 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_investor")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_investor = load8.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_investor");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });



        Dataset<Row> load9 = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://192.168.0.191:3306/prism")
                .option("dbtable", "company_staff")
                .option("user", "root")
                .option("password", "root").load();

        JavaPairRDD<String, Map<String, Object>> company_staff = load9.javaRDD().mapToPair(new PairFunction<Row, String, Map<String, Object>>() {
            private static final long serialVersionUID = -1964392616303257605L;

            @Override
            public Tuple2<String, Map<String, Object>> call(Row row) throws Exception {
                Map<String, Object> returnMap = new HashMap<String, Object>();
                StructType schema = row.schema();

                String[] fields = schema.fieldNames();
                int i = 0;
                for (String fieldname : fields) {

                    returnMap.put(fieldname, row.get(i));
                    i++;
                }

                returnMap.put("tablename", "company_staff");
                return new Tuple2<String, Map<String, Object>>(returnMap.get("company_id").toString(), returnMap);
            }
        });


        JavaPairRDD<String, Iterable<Map<String, Object>>> mainRDD = companyrdd.union(company_change_info)
                .union(company_abnormal_info)
                .union(company_category)
                .union(company_check_info)
                .union(company_equity_info)
                .union(company_punishment_info)
                .union(company_investor)
                .union(company_staff).groupByKey().filter(new Function<Tuple2<String, Iterable<Map<String, Object>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Iterable<Map<String, Object>>> v1) throws Exception {

                        Iterator<Map<String, Object>> iterator = v1._2().iterator();

                        while (iterator.hasNext()) {
                            Map<String, Object> next = iterator.next();
                            if ("company".equalsIgnoreCase(next.get("tablename").toString())) {
                                return Boolean.TRUE;
                            }
                        }


                        return Boolean.FALSE;
                    }
                });

        return mainRDD;
    }
}
